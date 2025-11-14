using Microsoft.EntityFrameworkCore.Update;
using System.Text;

namespace Ivy.EntityFrameworkCore.BigQuery.Update.Internal
{
    public class BigQueryUpdateSqlGenerator : UpdateSqlGenerator, IBigQueryUpdateSqlGenerator
    {
        public BigQueryUpdateSqlGenerator(UpdateSqlGeneratorDependencies dependencies)
        : base(dependencies)
        { }

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
                    if (columnModification.UseCurrentValueParameter)
                    {
                        SqlGenerationHelper.GenerateParameterNamePlaceholder(
                            commandStringBuilder,
                            columnModification.ParameterName!);
                    }
                    else if (columnModification.IsWrite && columnModification.Value != null)
                    {
                        var typeMapping = columnModification.TypeMapping;
                        var value = typeMapping?.GenerateSqlLiteral(columnModification.Value);
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
