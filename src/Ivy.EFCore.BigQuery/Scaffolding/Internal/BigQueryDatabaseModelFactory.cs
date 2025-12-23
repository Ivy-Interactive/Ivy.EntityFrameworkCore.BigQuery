using Ivy.Data.BigQuery;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Scaffolding;
using Microsoft.EntityFrameworkCore.Scaffolding.Metadata;
using Microsoft.EntityFrameworkCore.Storage;
using System.Data;
using System.Data.Common;
using System.Text.RegularExpressions;

namespace Ivy.EntityFrameworkCore.BigQuery.Scaffolding.Internal
{
    public class BigQueryDatabaseModelFactory : DatabaseModelFactory
    {
        private const string _projectId = "";
        private readonly IRelationalTypeMappingSource _typeMappingSource;
        private readonly Dictionary<string, StructTypeInfo> _structTypes = new();

        public BigQueryDatabaseModelFactory(
            IDiagnosticsLogger<DbLoggerCategory.Scaffolding> logger,
            IRelationalTypeMappingSource typeMappingSource)
        {
            _typeMappingSource = typeMappingSource;
        }

        /// <summary>
        /// Information about a STRUCT type discovered during scaffolding
        /// </summary>
        private class StructTypeInfo
        {
            public string StoreType { get; set; } = string.Empty;
            public string ClassName { get; set; } = string.Empty;
            public List<StructFieldInfo> Fields { get; set; } = new();
        }

        private class StructFieldInfo
        {
            public string Name { get; set; } = string.Empty;
            public string Type { get; set; } = string.Empty;
            public bool IsNullable { get; set; }
            public string? ClrType { get; set; }
        }


        public override DatabaseModel Create(string connectionString, DatabaseModelFactoryOptions options)
        {
            using var connection = new BigQueryConnection(connectionString);
            return Create(connection, options);
        }

        public override DatabaseModel Create(DbConnection dbConnection, DatabaseModelFactoryOptions options)
        {
            var connection = (BigQueryConnection)dbConnection;
            var connectionStartedOpen = connection.State == ConnectionState.Open;
            if (!connectionStartedOpen)
            {
                connection.Open();
            }

            try
            {
                var databaseModel = new DatabaseModel
                {
                    DatabaseName = connection.Database
                };

                databaseModel.DatabaseName = connection.Database;

                var tables = GetTables(connection, databaseModel, options.Tables.ToList(), options.Schemas.ToList());

                foreach (var table in tables)
                {
                    databaseModel.Tables.Add(table);
                }

                GetKeys(connection, tables);

                // Add STRUCT types as fake tables so EF Core scaffolds them as separate classes
                if (_structTypes.Any())
                {
                    var firstTable = tables.FirstOrDefault();
                    var schema = firstTable?.Schema;

                    foreach (var (structDefinition, structInfo) in _structTypes)
                    {
                        var structTable = new DatabaseTable
                        {
                            Schema = schema,
                            Name = structInfo.ClassName,
                            Database = databaseModel
                        };

                        structTable["BigQuery:IsStructType"] = true;
                        structTable["BigQuery:StructDefinition"] = structDefinition;

                        foreach (var field in structInfo.Fields)
                        {
                            var column = new DatabaseColumn
                            {
                                Table = structTable,
                                Name = field.Name,
                                StoreType = field.Type,
                                IsNullable = field.IsNullable
                            };

                            structTable.Columns.Add(column);
                        }

                        databaseModel.Tables.Add(structTable);
                    }
                }

                return databaseModel;
            }
            finally
            {
                if (!connectionStartedOpen)
                {
                    connection.Close();
                }
            }
        }

        private List<DatabaseTable> GetTables(DbConnection connection, DatabaseModel databaseModel, IReadOnlyList<string> tableFilters, IReadOnlyList<string> schemaFilters)
        {
            var tables = new List<DatabaseTable>();

            var schemaFilter = schemaFilters.FirstOrDefault();

            using var dt = connection.GetSchema("Tables", new[] { null, schemaFilter });

            foreach (DataRow row in dt.Rows)
            {
                var tableSchema = (string)row["TABLE_SCHEMA"];
                var tableName = (string)row["TABLE_NAME"];
                var tableType = (string)row["TABLE_TYPE"];

                if (tableFilters.Any() && !tableFilters.Contains(tableName)) continue;

                var table = tableType switch
                {
                    "BASE TABLE" => new DatabaseTable(),
                    "VIEW" => new DatabaseView(),
                    _ => throw new ArgumentOutOfRangeException($"Unknown table_type '{tableType}' when scaffolding {tableSchema}.{tableName}")
                };

                table.Schema = tableSchema;
                table.Name = tableName;
                table.Database = databaseModel;
                

                tables.Add(table);
            }

            GetColumns(connection, tables);

            return tables;
        }

        private void GetColumns(DbConnection connection, IReadOnlyList<DatabaseTable> tables)
        {
            foreach (var table in tables)
            {
                using var dt = connection.GetSchema(
                    "Columns",
                    new[] { null, table.Schema, table.Name, null });

                foreach (DataRow row in dt.Rows)
                {
                    var isNullable = (bool)row["IS_NULLABLE"];
                    var dataType = (string)row["DATA_TYPE"];
                    var columnName = (string)row["COLUMN_NAME"];
                    var defaultValue = row["COLUMN_DEFAULT"] == DBNull.Value ? null : (string)row["COLUMN_DEFAULT"];

                    // BigQuery returns "NULL" string for columns without defaults
                    if (string.Equals(defaultValue, "NULL", StringComparison.OrdinalIgnoreCase))
                    {
                        defaultValue = null;
                    }

                    var ordinal = Convert.ToInt32(row["ORDINAL_POSITION"]);

                    var dbColumn = new DatabaseColumn
                    {
                        StoreType = dataType,
                        Name = columnName,
                        IsNullable = isNullable,
                        DefaultValueSql = defaultValue,
                        Table = table
                    };

                    if (dataType.StartsWith("STRUCT<", StringComparison.OrdinalIgnoreCase))
                    {
                        var structClassName = RegisterStructType(dataType, table.Name, columnName);
                        dbColumn[RelationalAnnotationNames.Comment] = $"STRUCT_type: {structClassName}";
                        dbColumn["BigQuery:IsStructColumn"] = true;
                        dbColumn["BigQuery:StructDefinition"] = dataType;
                        dbColumn["BigQuery:StructClassName"] = structClassName;
                    }

                    table.Columns.Add(dbColumn);
                }
            }
        }
        
        private void GetKeys(DbConnection connection, IReadOnlyList<DatabaseTable> tables)
        {
            if (!tables.Any())
            {
                return;
            }

            var schema = tables.First().Schema;
            var ddlMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            using (var command = connection.CreateCommand())
            {
                command.CommandText = $"SELECT table_name, ddl FROM `{schema}`.INFORMATION_SCHEMA.TABLES WHERE table_type IN('BASE TABLE', 'VIEW')";

                if (connection.State != ConnectionState.Open)
                {
                    connection.Open();
                }

                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var tableName = reader.GetString(0);
                        var ddl = reader.GetString(1);
                        if (!string.IsNullOrWhiteSpace(ddl))
                        {
                            ddlMap[tableName] = ddl;
                        }
                    }
                }
            }

            foreach (var table in tables)
            {
                if (!ddlMap.TryGetValue(table.Name, out var ddl))
                {
                    continue;
                }

                var pkMatch = Regex.Match(ddl, @"PRIMARY KEY\s*\((.*?)\)", RegexOptions.IgnoreCase);
                if (pkMatch.Success)
                {
                    var columnsGroup = pkMatch.Groups[1].Value;
                    var pkColumnNames = columnsGroup.Split(',').Select(s => s.Trim().Trim('`')).ToList();

                    var pkColumns = pkColumnNames
                        .Select(name => table.Columns.FirstOrDefault(c => c.Name.Equals(name, StringComparison.OrdinalIgnoreCase)))
                        .Where(c => c != null)
                        .ToList();

                    if (pkColumns.Count == pkColumnNames.Count)
                    {
                        var primaryKey = new DatabasePrimaryKey
                        {
                            Table = table,
                            Name = $"PK_{table.Name}"
                        };

                        foreach (var column in pkColumns)
                        {
                            primaryKey.Columns.Add(column);
                        }

                        table.PrimaryKey = primaryKey;
                    }
                }
                
                var fkMatches = Regex.Matches(ddl, @"FOREIGN KEY\s*\((.*?)\)\s*REFERENCES\s*(.*?)\s*\((.*?)\)", RegexOptions.IgnoreCase);
                
                foreach (Match fkMatch in fkMatches)
                {
                    var columnsGroup = fkMatch.Groups[1].Value;
                    var fkColumnNames = columnsGroup.Split(',').Select(s => s.Trim().Trim('`')).ToList();

                    var principalTableNameWithSchema = fkMatch.Groups[2].Value.Trim().Trim('`');
                    var principalTableParts = principalTableNameWithSchema.Split('.');
                    var principalTableName = principalTableParts.Last();
            
                    var principalTable = tables.FirstOrDefault(t => t.Name.Equals(principalTableName, StringComparison.OrdinalIgnoreCase));

                    if (principalTable == null)
                    {
                        continue;
                    }

                    var principalColumnsGroup = fkMatch.Groups[3].Value;
                    var principalColumnNames = principalColumnsGroup.Split(',').Select(s => s.Trim().Trim('`')).ToList();

                    var fkColumns = fkColumnNames
                        .Select(name => table.Columns.FirstOrDefault(c => c.Name.Equals(name, StringComparison.OrdinalIgnoreCase)))
                        .Where(c => c != null)
                        .ToList();

                    var principalColumns = principalColumnNames
                        .Select(name => principalTable.Columns.FirstOrDefault(c => c.Name.Equals(name, StringComparison.OrdinalIgnoreCase)))
                        .Where(c => c != null)
                        .ToList();

                    if (fkColumns.Count == fkColumnNames.Count && principalColumns.Count == principalColumnNames.Count)
                    {
                        var foreignKey = new DatabaseForeignKey
                        {
                            Table = table,
                            PrincipalTable = principalTable,
                            Name = $"FK_{table.Name}_{principalTable.Name}",
                            OnDelete = ReferentialAction.NoAction
                        };

                        for (int i = 0; i < fkColumns.Count; i++)
                        {
                            foreignKey.Columns.Add(fkColumns[i]);
                            foreignKey.PrincipalColumns.Add(principalColumns[i]);
                        }

                        table.ForeignKeys.Add(foreignKey);
                    }
                }
            }
        }

        /// <summary>
        /// Registers a STRUCT type for code generation and returns the generated class name
        /// </summary>
        private string RegisterStructType(string structDefinition, string tableName, string columnName)
        {
            if (_structTypes.TryGetValue(structDefinition, out var existing))
            {
                return existing.ClassName;
            }

            var className = GenerateStructClassName(tableName, columnName);

            var fields = ParseStructDefinition(structDefinition);

            var structInfo = new StructTypeInfo
            {
                StoreType = structDefinition,
                ClassName = className,
                Fields = fields
            };

            _structTypes[structDefinition] = structInfo;
            return className;
        }

        /// <summary>
        /// Generate a meaningful class name for a STRUCT type
        /// </summary>
        private string GenerateStructClassName(string tableName, string columnName)
        {
            var className = ToPascalCase(columnName);

            var baseName = className;
            var counter = 1;
            while (_structTypes.Values.Any(s => s.ClassName == className))
            {
                className = $"{baseName}{counter++}";
            }

            return className;
        }

        /// <summary>
        /// Parse STRUCT&lt;field1 TYPE1, field2 TYPE2&gt; definition
        /// </summary>
        private List<StructFieldInfo> ParseStructDefinition(string structDef)
        {
            var fields = new List<StructFieldInfo>();

            if (!structDef.StartsWith("STRUCT<", StringComparison.OrdinalIgnoreCase))
            {
                return fields;
            }

            var startIndex = "STRUCT<".Length;
            var endIndex = FindMatchingCloseBracket(structDef, startIndex - 1);

            if (endIndex == -1)
            {
                return fields;
            }

            var fieldsContent = structDef.Substring(startIndex, endIndex - startIndex);
            var fieldDefinitions = SplitStructFields(fieldsContent);

            foreach (var fieldDef in fieldDefinitions)
            {
                var trimmed = fieldDef.Trim();
                var parts = trimmed.Split(new[] { ' ' }, 2, StringSplitOptions.RemoveEmptyEntries);

                if (parts.Length != 2)
                {
                    continue;
                }

                var fieldName = parts[0];
                var fieldType = parts[1];

                var clrType = MapBigQueryTypeToClrType(fieldType);

                fields.Add(new StructFieldInfo
                {
                    Name = fieldName,
                    Type = fieldType,
                    IsNullable = true,
                    ClrType = clrType
                });
            }

            return fields;
        }

        private static List<string> SplitStructFields(string fieldsContent)
        {
            var fields = new List<string>();
            var currentField = new System.Text.StringBuilder();
            var depth = 0;

            for (var i = 0; i < fieldsContent.Length; i++)
            {
                var ch = fieldsContent[i];

                if (ch == '<')
                {
                    depth++;
                    currentField.Append(ch);
                }
                else if (ch == '>')
                {
                    depth--;
                    currentField.Append(ch);
                }
                else if (ch == ',' && depth == 0)
                {
                    fields.Add(currentField.ToString());
                    currentField.Clear();
                }
                else
                {
                    currentField.Append(ch);
                }
            }

            if (currentField.Length > 0)
            {
                fields.Add(currentField.ToString());
            }

            return fields;
        }

        private static int FindMatchingCloseBracket(string str, int openIndex)
        {
            var depth = 1;
            for (var i = openIndex + 1; i < str.Length; i++)
            {
                if (str[i] == '<')
                {
                    depth++;
                }
                else if (str[i] == '>')
                {
                    depth--;
                    if (depth == 0)
                    {
                        return i;
                    }
                }
            }
            return -1;
        }

        private static string MapBigQueryTypeToClrType(string bigQueryType)
        {
            // Nested
            if (bigQueryType.StartsWith("STRUCT<", StringComparison.OrdinalIgnoreCase))
            {
                return "object"; 
            }

            if (bigQueryType.StartsWith("ARRAY<", StringComparison.OrdinalIgnoreCase))
            {
                return "System.Collections.Generic.List<object>";
            }

            return bigQueryType.ToUpperInvariant() switch
            {
                "STRING" => "string",
                "INT64" or "INTEGER" => "long",
                "FLOAT64" or "FLOAT" => "double",
                "BOOL" or "BOOLEAN" => "bool",
                "BYTES" => "byte[]",
                "TIMESTAMP" => "System.DateTimeOffset",
                "DATETIME" => "System.DateTime",
                "DATE" => "System.DateOnly",
                "TIME" => "System.TimeOnly",
                "NUMERIC" => "decimal",
                "BIGNUMERIC" => "decimal",
                _ when bigQueryType.StartsWith("NUMERIC(", StringComparison.OrdinalIgnoreCase) => "decimal",
                _ when bigQueryType.StartsWith("BIGNUMERIC(", StringComparison.OrdinalIgnoreCase) => "decimal",
                _ => "object"
            };
        }

        private static string ToPascalCase(string input)
        {
            if (string.IsNullOrEmpty(input))
            {
                return input;
            }

            var parts = input.Split('_', StringSplitOptions.RemoveEmptyEntries);
            var result = new System.Text.StringBuilder();

            foreach (var part in parts)
            {
                if (part.Length > 0)
                {
                    result.Append(char.ToUpperInvariant(part[0]));
                    if (part.Length > 1)
                    {
                        result.Append(part.Substring(1));
                    }
                }
            }

            return result.ToString();
        }

        private string SerializeStructTypes()
        {
            // Use System.Text.Json for serialization
            return System.Text.Json.JsonSerializer.Serialize(_structTypes);
        }
    }
}
