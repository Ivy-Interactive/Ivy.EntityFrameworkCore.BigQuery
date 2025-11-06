using Ivy.Data.BigQuery;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
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

        public BigQueryDatabaseModelFactory(
            IDiagnosticsLogger<DbLoggerCategory.Scaffolding> logger,
            IRelationalTypeMappingSource typeMappingSource)
        {
            _typeMappingSource = typeMappingSource;
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
    }
}
