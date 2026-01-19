using Ivy.EntityFrameworkCore.BigQuery.Storage.Internal.Mapping;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;
using System.Text;

namespace Ivy.EntityFrameworkCore.BigQuery.Update.Internal
{
    public class BigQueryUpdateSqlGenerator : UpdateSqlGenerator, IBigQueryUpdateSqlGenerator
    {
        public BigQueryUpdateSqlGenerator(UpdateSqlGeneratorDependencies dependencies)
        : base(dependencies)
        { }

        /// <summary>
        /// Generates SQL literal, handling types that don't work with ValueConverter.Sanitize, which uses
        /// Convert.ChangeType, which fails for types that don't implement IConvertible (enums, TimeSpan, DateOnly, TimeOnly, etc.)
        /// </summary>
        private static string? GenerateLiteral(RelationalTypeMapping? typeMapping, object? value)
        {
            if (value == null || typeMapping == null)
                return null;

            var valueType = value.GetType();

            if (valueType.IsEnum)
            {
                var numericValue = Convert.ChangeType(value, Enum.GetUnderlyingType(valueType));
                return numericValue.ToString();
            }

            // [Column(TypeName = "int")] bool BoolField)
            if (valueType == typeof(bool))
            {
                var storeType = typeMapping.StoreType?.ToUpperInvariant() ?? "";
                if (storeType.Contains("INT"))
                {
                    return (bool)value ? "1" : "0";
                }
            }

            try
            {
                return typeMapping.GenerateSqlLiteral(value);
            }
            // Doesn't implement IConvertible
            catch (InvalidCastException)
            {                
                return typeMapping.GenerateProviderValueSqlLiteral(value);
            }
        }

        protected override void AppendUpdateCommand(
        StringBuilder commandStringBuilder,
        string name,
        string? schema,
        IReadOnlyList<IColumnModification> writeOperations,
        IReadOnlyList<IColumnModification> readOperations,
        IReadOnlyList<IColumnModification> conditionOperations,
        bool appendReturningOneClause = false)
        {
            AppendUpdateCommandHeader(commandStringBuilder, name, schema, writeOperations);
            AppendWhereClause(commandStringBuilder, conditionOperations);
            // Skip RETURNING
            commandStringBuilder.AppendLine(SqlGenerationHelper.StatementTerminator);
        }

        protected override void AppendUpdateCommandHeader(
            StringBuilder commandStringBuilder,
            string name,
            string? schema,
            IReadOnlyList<IColumnModification> operations)
        {
            var hasStructColumn = operations.Any(o => o.TypeMapping is BigQueryStructTypeMapping);

            commandStringBuilder.Append("UPDATE ");
            SqlGenerationHelper.DelimitIdentifier(commandStringBuilder, name, schema);
            commandStringBuilder.AppendLine();
            commandStringBuilder.Append("SET ");

            for (var i = 0; i < operations.Count; i++)
            {
                var modification = operations[i];
                if (i > 0)
                {
                    commandStringBuilder.Append(", ");
                }

                SqlGenerationHelper.DelimitIdentifier(commandStringBuilder, modification.ColumnName);
                commandStringBuilder.Append(" = ");

                // If ANY column is a STRUCT, use literals for ALL columns
                if (hasStructColumn || modification.TypeMapping is BigQueryStructTypeMapping)
                {
                    if (modification.Value != null)
                    {
                        var literal = GenerateLiteral(modification.TypeMapping, modification.Value);
                        commandStringBuilder.Append(literal ?? "NULL");
                    }
                    else
                    {
                        commandStringBuilder.Append("NULL");
                    }
                }
                else if (modification.UseCurrentValueParameter)
                {
                    SqlGenerationHelper.GenerateParameterNamePlaceholder(
                        commandStringBuilder,
                        modification.ParameterName!);
                }
                else if (modification.Value != null)
                {
                    var literal = GenerateLiteral(modification.TypeMapping, modification.Value);
                    commandStringBuilder.Append(literal ?? "NULL");
                }
                else
                {
                    commandStringBuilder.Append("DEFAULT");
                }
            }
        }

        public override ResultSetMapping AppendInsertOperation(
            StringBuilder commandStringBuilder,
            IReadOnlyModificationCommand command,
            int commandPosition,
            out bool requiresTransaction)
        {
            var name = command.TableName;
            var schema = command.Schema;
            var writeOperations = command.ColumnModifications.Where(o => o.IsWrite).ToList();

            requiresTransaction = false;

            AppendInsertCommandHeader(commandStringBuilder, name, schema, writeOperations);
            AppendValuesHeader(commandStringBuilder, writeOperations);
            AppendValues(commandStringBuilder, name, schema, writeOperations);
            // Skip RETURNING
            commandStringBuilder.AppendLine(SqlGenerationHelper.StatementTerminator);

            return ResultSetMapping.NoResults;
        }

        protected override void AppendValues(
            StringBuilder commandStringBuilder,
            string name,
            string? schema,
            IReadOnlyList<IColumnModification> operations)
        {
            if (operations.Count > 0)
            {
                var hasStructColumn = operations.Any(o => o.TypeMapping is BigQueryStructTypeMapping);

                // Note: AppendValuesHeader already adds "VALUES ", so we just add "("
                commandStringBuilder.Append("(");

                for (var i = 0; i < operations.Count; i++)
                {
                    var modification = operations[i];
                    if (i > 0)
                    {
                        commandStringBuilder.Append(", ");
                    }

                    // If ANY column is a STRUCT, use literals for ALL columns
                    if (hasStructColumn || modification.TypeMapping is BigQueryStructTypeMapping)
                    {
                        if (modification.Value != null)
                        {
                            var literal = GenerateLiteral(modification.TypeMapping, modification.Value);
                            commandStringBuilder.Append(literal ?? "NULL");
                        }
                        else
                        {
                            commandStringBuilder.Append("NULL");
                        }
                    }
                    else if (modification.UseCurrentValueParameter)
                    {
                        SqlGenerationHelper.GenerateParameterNamePlaceholder(
                            commandStringBuilder,
                            modification.ParameterName!);
                    }
                    else if (modification.Value != null)
                    {
                        var literal = GenerateLiteral(modification.TypeMapping, modification.Value);
                        commandStringBuilder.Append(literal ?? "NULL");
                    }
                    else
                    {
                        commandStringBuilder.Append("DEFAULT");
                    }
                }

                commandStringBuilder.Append(')');
            }
        }

        public override ResultSetMapping AppendDeleteOperation(
            StringBuilder commandStringBuilder,
            IReadOnlyModificationCommand command,
            int commandPosition,
            out bool requiresTransaction)
        {
            var name = command.TableName;
            var schema = command.Schema;
            var conditionOperations = command.ColumnModifications.Where(o => o.IsCondition).ToList();

            requiresTransaction = false;

            AppendDeleteCommandHeader(commandStringBuilder, name, schema);
            AppendWhereClause(commandStringBuilder, conditionOperations);
            // Skip RETURNING
            commandStringBuilder.AppendLine(SqlGenerationHelper.StatementTerminator);

            return ResultSetMapping.NoResults;
        }

        public override ResultSetMapping AppendUpdateOperation(
            StringBuilder commandStringBuilder,
            IReadOnlyModificationCommand command,
            int commandPosition,
            out bool requiresTransaction)
        {
            var name = command.TableName;
            var schema = command.Schema;
            var writeOperations = command.ColumnModifications.Where(o => o.IsWrite).ToList();
            var conditionOperations = command.ColumnModifications.Where(o => o.IsCondition).ToList();

            requiresTransaction = false;

            AppendUpdateCommandHeader(commandStringBuilder, name, schema, writeOperations);
            AppendWhereClause(commandStringBuilder, conditionOperations);
            // Skip RETURNING
            commandStringBuilder.AppendLine(SqlGenerationHelper.StatementTerminator);

            return ResultSetMapping.NoResults;
        }

        protected override void AppendWhereClause(
            StringBuilder commandStringBuilder,
            IReadOnlyList<IColumnModification> operations)
        {
            if (operations.Count == 0)
            {
                commandStringBuilder
                    .AppendLine()
                    .Append("WHERE true");
            }
            else
            {
                base.AppendWhereClause(commandStringBuilder, operations);
            }
        }

        public virtual ResultSetMapping AppendBulkInsertOperation(
            StringBuilder commandStringBuilder,
            IReadOnlyList<IReadOnlyModificationCommand> modificationCommands,
            int commandPosition,
            out bool requiresTransaction)
        {
            if (modificationCommands.Count == 0)
            {
                throw new ArgumentException("Modification commands cannot be empty", nameof(modificationCommands));
            }

            var firstCommand = modificationCommands[0];
            var name = firstCommand.TableName;
            var schema = firstCommand.Schema;
            var writeOperations = firstCommand.ColumnModifications.Where(o => o.IsWrite).ToList();
            var readOperations = firstCommand.ColumnModifications.Where(o => o.IsRead).ToList();

            requiresTransaction = false;

            commandStringBuilder.Append("INSERT INTO ");
            SqlGenerationHelper.DelimitIdentifier(commandStringBuilder, name, schema);

            commandStringBuilder.Append(" (");
            for (var i = 0; i < writeOperations.Count; i++)
            {
                if (i > 0)
                {
                    commandStringBuilder.Append(", ");
                }
                SqlGenerationHelper.DelimitIdentifier(commandStringBuilder, writeOperations[i].ColumnName);
            }
            commandStringBuilder.AppendLine(")");

            commandStringBuilder.Append("VALUES ");

            var hasStructColumn = modificationCommands.Any(cmd =>
                cmd.ColumnModifications.Any(o => o.IsWrite && o.TypeMapping is BigQueryStructTypeMapping));

            for (var commandIndex = 0; commandIndex < modificationCommands.Count; commandIndex++)
            {
                if (commandIndex > 0)
                {
                    commandStringBuilder.AppendLine(",");
                    commandStringBuilder.Append("       ");
                }

                var command = modificationCommands[commandIndex];
                var currentWriteOperations = command.ColumnModifications.Where(o => o.IsWrite).ToList();

                commandStringBuilder.Append('(');
                for (var i = 0; i < currentWriteOperations.Count; i++)
                {
                    if (i > 0)
                    {
                        commandStringBuilder.Append(", ");
                    }

                    var columnModification = currentWriteOperations[i];

                    // If ANY command has a STRUCT, use literals for ALL values
                    if (hasStructColumn || columnModification.TypeMapping is BigQueryStructTypeMapping)
                    {
                        if (columnModification.IsWrite && columnModification.Value != null)
                        {
                            var value = GenerateLiteral(columnModification.TypeMapping, columnModification.Value);
                            commandStringBuilder.Append(value ?? "NULL");
                        }
                        else
                        {
                            commandStringBuilder.Append("NULL");
                        }
                    }
                    else if (columnModification.UseCurrentValueParameter)
                    {
                        SqlGenerationHelper.GenerateParameterNamePlaceholder(
                            commandStringBuilder,
                            columnModification.ParameterName!);
                    }
                    else if (columnModification.IsWrite && columnModification.Value != null)
                    {
                        var value = GenerateLiteral(columnModification.TypeMapping, columnModification.Value);
                        commandStringBuilder.Append(value ?? "NULL");
                    }
                    else
                    {
                        commandStringBuilder.Append("DEFAULT");
                    }
                }
                commandStringBuilder.Append(')');
            }

            commandStringBuilder.AppendLine(SqlGenerationHelper.StatementTerminator);

            //No RETURNING
            return ResultSetMapping.NoResults;
        }

    }
}
