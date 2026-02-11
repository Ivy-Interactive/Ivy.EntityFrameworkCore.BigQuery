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
        /// Determines if a type mapping requires literal values instead of parameters.
        /// Needed for STRUCT and ARRAY&lt;STRUCT&gt; types because the BQ SDK doesn't support STRUCT parameters
        /// </summary>
        private static bool RequiresLiteralValue(RelationalTypeMapping? typeMapping)
        {
            if (typeMapping is BigQueryStructTypeMapping)
                return true;

            // ARRAY<STRUCT>
            if (typeMapping is BigQueryArrayTypeMapping arrayMapping
                && arrayMapping.ElementTypeMapping is BigQueryStructTypeMapping)
                return true;

            return false;
        }

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
            var hasStructColumn = operations.Any(o => RequiresLiteralValue(o.TypeMapping));

            commandStringBuilder.Append("UPDATE ");
            SqlGenerationHelper.DelimitIdentifier(commandStringBuilder, name, schema);
            commandStringBuilder.AppendLine();
            commandStringBuilder.Append("SET ");

            // Group operations by column name to handle multiple JSON path updates to the same column
            var groupedOperations = operations
                .GroupBy(o => o.ColumnName)
                .ToList();

            for (var groupIndex = 0; groupIndex < groupedOperations.Count; groupIndex++)
            {
                var group = groupedOperations[groupIndex];
                if (groupIndex > 0)
                {
                    commandStringBuilder.Append(", ");
                }

                SqlGenerationHelper.DelimitIdentifier(commandStringBuilder, group.Key);
                commandStringBuilder.Append(" = ");

                var jsonPathOps = group.Where(o => o.JsonPath is not (null or "$")).ToList();
                var nonJsonPathOps = group.Where(o => o.JsonPath is null or "$").ToList();

                if (jsonPathOps.Count > 0)
                {
                    // Use JSON_SET for JSON path updates
                    // BigQuery syntax: JSON_SET(json_expr, path1, value1, path2, value2, ...)
                    AppendJsonSetExpression(commandStringBuilder, group.Key, jsonPathOps, hasStructColumn);
                }
                else
                {
                    // Regular update (non-JSON or root JSON replacement)
                    var modification = nonJsonPathOps.First();
                    AppendColumnValue(commandStringBuilder, modification, hasStructColumn);
                }
            }
        }

        /// <summary>
        /// Appends a JSON_SET expression for partial JSON updates.
        /// BigQuery syntax: JSON_SET(column, '$.path1', value1, '$.path2', value2, ...)
        /// </summary>
        private void AppendJsonSetExpression(
            StringBuilder commandStringBuilder,
            string columnName,
            IReadOnlyList<IColumnModification> jsonPathOperations,
            bool hasStructColumn)
        {
            commandStringBuilder.Append("JSON_SET(");
            SqlGenerationHelper.DelimitIdentifier(commandStringBuilder, columnName);

            foreach (var modification in jsonPathOperations)
            {
                commandStringBuilder.Append(", '");
                commandStringBuilder.Append(modification.JsonPath);
                commandStringBuilder.Append("', ");

                // For JSON path updates, we need special handling because the column's
                // type mapping is JSON, but we're updating individual primitive values.
                // Use JSON-compatible literals instead of parameters for primitive types.
                if (TryAppendJsonPrimitiveLiteral(commandStringBuilder, modification))
                {
                    continue;
                }

                // Determine if we need PARSE_JSON for the value
                // Complex objects/arrays come as JSON strings and need to be parsed
                var needsParseJson = NeedsParseJson(modification);

                if (needsParseJson)
                {
                    commandStringBuilder.Append("PARSE_JSON(");
                }

                AppendColumnValue(commandStringBuilder, modification, hasStructColumn);

                if (needsParseJson)
                {
                    commandStringBuilder.Append(")");
                }
            }

            commandStringBuilder.Append(")");
        }

        /// <summary>
        /// Tries to append a JSON-compatible literal for primitive values in JSON path updates.
        /// Returns true if a literal was appended, false if standard parameter handling should be used.
        /// </summary>
        private bool TryAppendJsonPrimitiveLiteral(StringBuilder commandStringBuilder, IColumnModification modification)
        {
            // Only use literal handling for JSON-typed columns with non-JSON property values
            if (modification.TypeMapping?.StoreType != "JSON" || modification.Property == null)
            {
                return false;
            }

            var value = modification.Value;

            // Handle null values
            if (value == null || value == DBNull.Value)
            {
                commandStringBuilder.Append("NULL");
                return true;
            }

            // Check the actual value type since EF Core may have already serialized to string
            var valueType = value.GetType();

            // If the value is already a string (EF Core serialized it), output it as a quoted literal
            if (valueType == typeof(string))
            {
                var strValue = (string)value;
                commandStringBuilder.Append('\'');
                commandStringBuilder.Append(strValue.Replace("'", "''"));
                commandStringBuilder.Append('\'');
                return true;
            }

            // Handle DateTime types - need to be JSON string literals
            if (valueType == typeof(DateTime))
            {
                var dt = (DateTime)value;
                commandStringBuilder.Append('\'');
                commandStringBuilder.Append(dt.ToString("yyyy-MM-ddTHH:mm:ss"));
                commandStringBuilder.Append('\'');
                return true;
            }

            if (valueType == typeof(DateTimeOffset))
            {
                var dt = (DateTimeOffset)value;
                commandStringBuilder.Append('\'');
                commandStringBuilder.Append(dt.ToString("yyyy-MM-ddTHH:mm:sszzz"));
                commandStringBuilder.Append('\'');
                return true;
            }

            if (valueType == typeof(DateOnly))
            {
                var d = (DateOnly)value;
                commandStringBuilder.Append('\'');
                commandStringBuilder.Append(d.ToString("yyyy-MM-dd"));
                commandStringBuilder.Append('\'');
                return true;
            }

            if (valueType == typeof(TimeOnly))
            {
                var t = (TimeOnly)value;
                commandStringBuilder.Append('\'');
                commandStringBuilder.Append(t.ToString("HH:mm:ss.FFFFFFF"));
                commandStringBuilder.Append('\'');
                return true;
            }

            if (valueType == typeof(TimeSpan))
            {
                var ts = (TimeSpan)value;
                commandStringBuilder.Append('\'');
                commandStringBuilder.Append(ts.ToString(@"hh\:mm\:ss\.fffffff"));
                commandStringBuilder.Append('\'');
                return true;
            }

            if (valueType == typeof(Guid))
            {
                var g = (Guid)value;
                commandStringBuilder.Append('\'');
                commandStringBuilder.Append(g.ToString());
                commandStringBuilder.Append('\'');
                return true;
            }

            // Handle numeric types - use as-is
            if (valueType == typeof(int) || valueType == typeof(long) || valueType == typeof(short) ||
                valueType == typeof(byte) || valueType == typeof(sbyte) ||
                valueType == typeof(uint) || valueType == typeof(ulong) || valueType == typeof(ushort))
            {
                commandStringBuilder.Append(value.ToString());
                return true;
            }

            if (valueType == typeof(decimal))
            {
                commandStringBuilder.Append(((decimal)value).ToString(System.Globalization.CultureInfo.InvariantCulture));
                return true;
            }

            if (valueType == typeof(double))
            {
                commandStringBuilder.Append(((double)value).ToString(System.Globalization.CultureInfo.InvariantCulture));
                return true;
            }

            if (valueType == typeof(float))
            {
                commandStringBuilder.Append(((float)value).ToString(System.Globalization.CultureInfo.InvariantCulture));
                return true;
            }

            // Handle bool
            if (valueType == typeof(bool))
            {
                commandStringBuilder.Append((bool)value ? "true" : "false");
                return true;
            }

            // Handle enums - convert to their string representation
            if (valueType.IsEnum)
            {
                commandStringBuilder.Append('\'');
                commandStringBuilder.Append(value.ToString());
                commandStringBuilder.Append('\'');
                return true;
            }

            // For other types, fall back to standard handling
            return false;
        }

        /// <summary>
        /// Determines if the JSON value needs to be wrapped with PARSE_JSON.
        /// This is needed for complex objects/arrays that are serialized as JSON strings.
        /// </summary>
        private static bool NeedsParseJson(IColumnModification modification)
        {
            // If the type mapping already produces JSON-typed parameters, we don't need PARSE_JSON.
            // BigQueryOwnedJsonTypeMapping and BigQueryJsonTypeMapping set the parameter type to JSON,
            // so BigQuery will already interpret the value as JSON.
            if (modification.TypeMapping?.StoreType == "JSON")
            {
                return false;
            }

            // If there's no property, it's a full entity update (JSON object/array)
            if (modification.Property == null)
            {
                return true;
            }

            // Check if the property type is a collection or complex type
            var clrType = modification.Property.ClrType;

            // Primitive types don't need PARSE_JSON
            if (clrType == typeof(string) ||
                clrType == typeof(bool) || clrType == typeof(bool?) ||
                clrType == typeof(int) || clrType == typeof(int?) ||
                clrType == typeof(long) || clrType == typeof(long?) ||
                clrType == typeof(short) || clrType == typeof(short?) ||
                clrType == typeof(byte) || clrType == typeof(byte?) ||
                clrType == typeof(sbyte) || clrType == typeof(sbyte?) ||
                clrType == typeof(uint) || clrType == typeof(uint?) ||
                clrType == typeof(ulong) || clrType == typeof(ulong?) ||
                clrType == typeof(ushort) || clrType == typeof(ushort?) ||
                clrType == typeof(float) || clrType == typeof(float?) ||
                clrType == typeof(double) || clrType == typeof(double?) ||
                clrType == typeof(decimal) || clrType == typeof(decimal?) ||
                clrType == typeof(char) || clrType == typeof(char?) ||
                clrType == typeof(DateTime) || clrType == typeof(DateTime?) ||
                clrType == typeof(DateTimeOffset) || clrType == typeof(DateTimeOffset?) ||
                clrType == typeof(DateOnly) || clrType == typeof(DateOnly?) ||
                clrType == typeof(TimeOnly) || clrType == typeof(TimeOnly?) ||
                clrType == typeof(TimeSpan) || clrType == typeof(TimeSpan?) ||
                clrType == typeof(Guid) || clrType == typeof(Guid?) ||
                clrType.IsEnum)
            {
                return false;
            }

            // Collections and complex types need PARSE_JSON
            return true;
        }

        /// <summary>
        /// Appends a column value (parameter placeholder or literal).
        /// </summary>
        private void AppendColumnValue(
            StringBuilder commandStringBuilder,
            IColumnModification modification,
            bool hasStructColumn)
        {
            // If ANY column is a STRUCT or ARRAY<STRUCT>, use literals for ALL columns
            // (Google BigQuery SDK doesn't support STRUCT parameters)
            if (hasStructColumn || RequiresLiteralValue(modification.TypeMapping))
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
                commandStringBuilder.Append("NULL");
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
                var hasStructColumn = operations.Any(o => RequiresLiteralValue(o.TypeMapping));

                // Note: AppendValuesHeader already adds "VALUES ", so we just add "("
                commandStringBuilder.Append("(");

                for (var i = 0; i < operations.Count; i++)
                {
                    var modification = operations[i];
                    if (i > 0)
                    {
                        commandStringBuilder.Append(", ");
                    }

                    // If ANY column is a STRUCT or ARRAY<STRUCT>, use literals for ALL columns
                    // (Google BigQuery SDK doesn't support STRUCT parameters)
                    if (hasStructColumn || RequiresLiteralValue(modification.TypeMapping))
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
                cmd.ColumnModifications.Any(o => o.IsWrite && RequiresLiteralValue(o.TypeMapping)));

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

                    // If ANY command has a STRUCT or ARRAY<STRUCT>, use literals for ALL values
                    // (Google BigQuery SDK doesn't support STRUCT parameters)
                    if (hasStructColumn || RequiresLiteralValue(columnModification.TypeMapping))
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