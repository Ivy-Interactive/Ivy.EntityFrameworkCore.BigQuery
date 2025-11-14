using Google.Apis.Auth.OAuth2;
using Google.Cloud.BigQuery.V2;
using System.Data;
using System.Data.Common;
using System.Text;

namespace Ivy.Data.BigQuery
{

    public class BigQueryConnection : DbConnection
    {
        private string _connectionString = string.Empty;
        private ConnectionState _state = ConnectionState.Closed;
        private BigQueryClient _client;
        private readonly Dictionary<string, string> _parsedConnectionString = new(StringComparer.OrdinalIgnoreCase);

        private string _dataSource;
        private string _projectId;
        private string _defaultDatasetId;
        private string _location;
        private string _credentialPath;
        private string _credentialJson;
        private bool _useAdc;
        private int _connectionTimeoutSeconds = 15;
        private bool _isDisposed = false;

        public override event StateChangeEventHandler StateChange;

        public BigQueryConnection() { }

        public BigQueryConnection(string connectionString)
        {
            ConnectionString = connectionString;
        }

        /// <summary>
        /// Gets or sets the string used to open the connection.
        /// Format: "ProjectId=your-project;DefaultDatasetId=your_dataset;AuthMethod=JsonCredentials;CredentialsFile=/path/to/key.json;Timeout=30"
        /// Or: "ProjectId=your-project;AuthMethod=ApplicationDefaultCredentials;"
        /// Or: "ProjectId=your-project;AuthMethod=JsonCredentials;JsonCredentials={...json...}"
        /// Or: "ProjectId=your-project;" (defaults to ApplicationDefaultCredentials)
        /// Supported Keys:
        /// - ProjectId (Required)
        /// - DefaultDatasetId (Optional)
        /// - AuthMethod (Optional): 'JsonCredentials' or 'ApplicationDefaultCredentials'. Defaults to 'ApplicationDefaultCredentials' if not specified.
        /// - CredentialsFile (Required if AuthMethod=JsonCredentials and JsonCredentials not provided): Path to the JSON service account key file.
        /// - JsonCredentials (Required if AuthMethod=JsonCredentials and CredentialsFile not provided): JSON service account credentials as a string.
        /// - Timeout (Optional): Seconds to wait for connection/authentication (default 15).
        /// </summary>
        public override string ConnectionString
        {
            get => _connectionString;
            set
            {
                if (State != ConnectionState.Closed)
                {
                    throw new InvalidOperationException("Cannot change ConnectionString while the connection is open.");
                }
                _connectionString = value ?? string.Empty;
                ParseConnectionString();
            }
        }

        /// <summary>
        /// Default is 15 seconds.
        /// </summary>
        public override int ConnectionTimeout => _connectionTimeoutSeconds;

        public override string Database => _defaultDatasetId ?? string.Empty;

        public override ConnectionState State => _state;

        public override string DataSource => _dataSource ?? string.Empty;

        public override string ServerVersion => typeof(BigQueryClient).Assembly.GetName().Version?.ToString() ?? "Google.Cloud.BigQuery.V2";

        internal BigQueryClient Client => _client;

        public string DefaultProjectId => _projectId;

        public string DefaultDatasetId => _defaultDatasetId;

        public string Location => _location;

        /// <summary>
        /// Gets the underlying BigQueryClient for advanced scenarios.
        /// </summary>
        public BigQueryClient GetBigQueryClient()
        {
            if (State != ConnectionState.Open)
            {
                throw new InvalidOperationException(
                    "The connection must be open to access the BigQuery client. Call Open() or OpenAsync() first.");
            }

            if (_client == null)
            {
                throw new InvalidOperationException(
                    "The BigQuery client is not available. Ensure the connection was opened successfully.");
            }

            return _client;
        }

        protected override DbProviderFactory DbProviderFactory => BigQueryProviderFactory.Instance;

        public override async Task OpenAsync(CancellationToken cancellationToken)
        {
            VerifyNotDisposed();
            cancellationToken.ThrowIfCancellationRequested();

            if (State == ConnectionState.Open)
            {
                throw new InvalidOperationException("Connection is already open.");
            }
            if (string.IsNullOrWhiteSpace(_projectId))
            {
                throw new InvalidOperationException("ProjectId must be specified in the connection string.");
            }

            var previousState = SetState(ConnectionState.Connecting);

            try
            {
                GoogleCredential credential = null;

                var authMethod = _parsedConnectionString.GetValueOrDefault("AuthMethod");
                if (string.Equals(authMethod, "JsonCredentials", StringComparison.OrdinalIgnoreCase))
                {
                    if (!string.IsNullOrWhiteSpace(_credentialJson))
                    {
                        credential = GoogleCredential.FromJson(_credentialJson);
                    }
                    else if (!string.IsNullOrWhiteSpace(_credentialPath))
                    {
                        if (!File.Exists(_credentialPath))
                        {
                            throw new FileNotFoundException("Credentials JSON file not found.", _credentialPath);
                        }

                        await using var stream = new FileStream(_credentialPath, FileMode.Open, FileAccess.Read);
                        credential = await GoogleCredential.FromStreamAsync(stream, cancellationToken);
                    }
                    else
                    {
                        throw new InvalidOperationException("Either CredentialsFile or JsonCredentials must be specified when AuthMethod is JsonCredentials.");
                    }
                }
                else if (string.Equals(authMethod, "ApplicationDefaultCredentials", StringComparison.OrdinalIgnoreCase)
                    || _useAdc
                    || string.IsNullOrWhiteSpace(authMethod))
                {
                    // Default to ApplicationDefaultCredentials if AuthMethod is not specified or explicitly set
                    credential = await GoogleCredential.GetApplicationDefaultAsync(cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    throw new InvalidOperationException($"Invalid AuthMethod '{authMethod}'. Must be either 'JsonCredentials' or 'ApplicationDefaultCredentials'.");
                }

                var clientBuilder = new BigQueryClientBuilder
                {
                    ProjectId = _projectId,
                    Credential = credential,
                };
                if (!string.IsNullOrWhiteSpace(_location))
                {
                    clientBuilder.DefaultLocation = _location;
                }
                if (!string.IsNullOrWhiteSpace(DataSource))
                {
                    clientBuilder.BaseUri = DataSource;
                }


                _client = await clientBuilder.BuildAsync(cancellationToken).ConfigureAwait(false);

                SetState(ConnectionState.Open);
            }
            catch (Exception ex)
            {
                _client = null;
                SetState(ConnectionState.Closed);

                throw new BigQueryException($"Failed to open connection: {ex.Message}", ex);
            }
        }

        public override void Open()
        {
            try
            {
                OpenAsync(CancellationToken.None).GetAwaiter().GetResult();
            }
            catch (AggregateException ae) when (ae.InnerExceptions.Count == 1)
            {
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(ae.InnerException).Throw();
                throw;
            }
        }

        public override void Close()
        {
            if (_state == ConnectionState.Closed)
            {
                return;
            }

            _client?.Dispose();
            _client = null;

            SetState(ConnectionState.Closed);
        }

        public override void ChangeDatabase(string databaseName)
        {
            if (State != ConnectionState.Open)
            {
                throw new InvalidOperationException("Cannot change database on a closed connection.");
            }
            if (string.IsNullOrWhiteSpace(databaseName))
            {
                throw new ArgumentException("Database name (Default Dataset) cannot be null or empty.", nameof(databaseName));
            }
            if (databaseName.Contains('.'))
            {
                throw new ArgumentException("Database name should only be the Dataset ID, not project.dataset.", nameof(databaseName));
            }

            _defaultDatasetId = databaseName;
        }

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            throw new NotSupportedException("BigQuery does not support ADO.NET style transactions. Use multi-statement queries/scripts within a single command for atomic operations.");
        }

        protected override DbCommand CreateDbCommand()
        {
            var command = new BigQueryCommand();
            command.Connection = this;
            return command;
        }

        public override DataTable GetSchema()
            => GetSchema(DbMetaDataCollectionNames.MetaDataCollections);

        public override DataTable GetSchema(string collectionName)
           => GetSchema(collectionName, []);

        public override DataTable GetSchema(string collectionName, string[]? restrictions)
        {
            EnsureConnectionOpen();

            return collectionName.ToUpperInvariant() switch
            {
                "TABLES" => GetTablesSchema(restrictions),
                "COLUMNS" => GetColumnsSchema(restrictions),
                "METADATACOLLECTIONS" => GetMetaDataCollectionsSchema(),
                "DATATYPES" => GetDataTypesSchema(),
                _ => throw new ArgumentOutOfRangeException(nameof(collectionName), collectionName, "Invalid collection name.")
            };
        }

        private static DataTable GetDataTypesSchema()
        {
            var dataTypesTable = new DataTable("DataTypes");

            dataTypesTable.Columns.Add("TypeName", typeof(string));
            dataTypesTable.Columns.Add("ProviderDbType", typeof(int)); 
            dataTypesTable.Columns.Add("DataType", typeof(string));
            dataTypesTable.Columns.Add("ColumnSize", typeof(long));
            dataTypesTable.Columns.Add("CreateFormat", typeof(string));
            dataTypesTable.Columns.Add("CreateParameters", typeof(string));
            dataTypesTable.Columns.Add("IsAutoincrement", typeof(bool));
            dataTypesTable.Columns.Add("IsFixedLength", typeof(bool));
            dataTypesTable.Columns.Add("IsFixedPrecisionAndScale", typeof(bool));
            dataTypesTable.Columns.Add("IsNullable", typeof(bool));
            dataTypesTable.Columns.Add("IsUnsigned", typeof(bool));

            Action<string, BigQueryDbType, Type, bool, bool, bool, string?, string?> addRow =
                (typeName, providerDbType, clrType, isFixedLength, isFixedPS, isUnsigned, createFormat, createParams) =>
                {
                    dataTypesTable.Rows.Add(
                    typeName,
                    (int)providerDbType,
                    clrType.FullName,
                    -1L,
                    createFormat,
                    createParams,
                    false,
                    isFixedLength,
                    isFixedPS,
                    true,
                    isUnsigned
                );
                };

            addRow("INT64", BigQueryDbType.Int64, typeof(long), true, true, false, null, null);
            addRow("FLOAT64", BigQueryDbType.Float64, typeof(double), true, true, false, null, null);
            addRow("BOOL", BigQueryDbType.Bool, typeof(bool), true, true, false, null, null);
            addRow("STRING", BigQueryDbType.String, typeof(string), false, false, false, null, null);
            addRow("BYTES", BigQueryDbType.Bytes, typeof(byte[]), false, false, false, null, null);
            addRow("TIMESTAMP", BigQueryDbType.Timestamp, typeof(DateTimeOffset), true, true, false, null, null);
            addRow("DATE", BigQueryDbType.Date, typeof(DateTime), true, true, false, null, null);
            addRow("TIME", BigQueryDbType.Time, typeof(TimeSpan), true, true, false, null, null);
            addRow("DATETIME", BigQueryDbType.DateTime, typeof(DateTime), true, true, false, null, null);
            addRow("GEOGRAPHY", BigQueryDbType.Geography, typeof(string), false, false, false, null, null);
            addRow("JSON", BigQueryDbType.Json, typeof(string), false, false, false, null, null);

            addRow("NUMERIC", BigQueryDbType.Numeric, typeof(BigQueryNumeric), true, false, false, "NUMERIC({0}, {1})", "precision, scale");
            addRow("BIGNUMERIC", BigQueryDbType.BigNumeric, typeof(BigQueryBigNumeric), true, false, false, "BIGNUMERIC({0}, {1})", "precision, scale");

            addRow("STRUCT", BigQueryDbType.Struct, typeof(IDictionary<string, object>), false, false, false, null, null);
            addRow("ARRAY", BigQueryDbType.Array, typeof(object[]), false, false, false, null, null);

            return dataTypesTable;
        }

        private static DataTable GetMetaDataCollectionsSchema()
        {

            return new DataTable(DbMetaDataCollectionNames.MetaDataCollections)
            {
                Columns =
                    {
                        { DbMetaDataColumnNames.CollectionName, typeof(string) },
                        { DbMetaDataColumnNames.NumberOfRestrictions, typeof(int) },
                        { DbMetaDataColumnNames.NumberOfIdentifierParts, typeof(int) }
                    },
                Rows =
                    {
                        new object[] { DbMetaDataCollectionNames.MetaDataCollections, 0, 0 },
                        //new object[] { DbMetaDataCollectionNames.ReservedWords, 0, 0 }
                    }
            };
        }

        private DataTable GetTablesSchema(string[]? restrictionValues)
        {
            string? datasetId = (restrictionValues?.Length > 1 && !string.IsNullOrWhiteSpace(restrictionValues[1])) ? restrictionValues[1] : DefaultDatasetId;

            if (string.IsNullOrWhiteSpace(datasetId))
            {
                throw new ArgumentException("A dataset must be specified either in the connection string ('DefaultDatasetId=...') or as the second restriction value for the 'Tables' schema collection.");
            }

            var queryBuilder = new StringBuilder();
            queryBuilder.Append($"SELECT table_catalog, table_schema, table_name, table_type FROM `{datasetId}`.INFORMATION_SCHEMA.TABLES");

            var whereClauses = new List<string>();
            var parameters = new List<BigQueryParameter>();

            whereClauses.Add("table_type IN('BASE TABLE', 'VIEW')");

            if (restrictionValues?.Length > 0 && !string.IsNullOrWhiteSpace(restrictionValues[0]))
            {
                whereClauses.Add("table_catalog = @catalog");
                parameters.Add(new BigQueryParameter("@catalog", BigQueryDbType.String, restrictionValues[0]));
            }

            if (restrictionValues?.Length > 2 && !string.IsNullOrWhiteSpace(restrictionValues[2]))
            {
                whereClauses.Add("table_name = @tableName");
                parameters.Add(new BigQueryParameter("@tableName", BigQueryDbType.String, restrictionValues[2]));
            }

            if (whereClauses.Any())
            {
                queryBuilder.Append(" WHERE ");
                queryBuilder.Append(string.Join(" AND ", whereClauses));
            }

            queryBuilder.Append(" ORDER BY table_schema, table_name");

            var resultTable = new DataTable("Tables");
            resultTable.Columns.Add("TABLE_CATALOG", typeof(string));
            resultTable.Columns.Add("TABLE_SCHEMA", typeof(string));
            resultTable.Columns.Add("TABLE_NAME", typeof(string));
            resultTable.Columns.Add("TABLE_TYPE", typeof(string));

            using (var command = (BigQueryCommand)this.CreateCommand())
            {
                command.CommandText = queryBuilder.ToString();
                foreach (var p in parameters)
                {
                    command.Parameters.Add(p);
                }

                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var newRow = resultTable.NewRow();
                        newRow["TABLE_CATALOG"] = reader.IsDBNull(0) ? null : reader.GetString(0);
                        newRow["TABLE_SCHEMA"] = reader.IsDBNull(1) ? null : reader.GetString(1);
                        newRow["TABLE_NAME"] = reader.IsDBNull(2) ? null : reader.GetString(2);
                        newRow["TABLE_TYPE"] = reader.IsDBNull(3) ? null : reader.GetString(3);
                        resultTable.Rows.Add(newRow);
                    }
                }
            }

            return resultTable;
        }

        private DataTable GetColumnsSchema(string[]? restrictionValues)
        {
            string? datasetId = (restrictionValues?.Length > 1) ? restrictionValues[1] : DefaultDatasetId;

            if (string.IsNullOrWhiteSpace(datasetId))
            {
                throw new ArgumentException("A dataset must be specified either in the connection string ('DefaultDatasetId=...') or as the second restriction value for the 'Columns' schema collection.");
            }

            var queryBuilder = new StringBuilder();
            queryBuilder.Append($"""
            SELECT
                table_catalog,
                table_schema,
                table_name,
                column_name,
                ordinal_position,
                column_default,
                is_nullable,
                data_type,
                CAST(REGEXP_EXTRACT(data_type, r'STRING\((\d+)\)') AS INT64) AS character_maximum_length,
                CAST(REGEXP_EXTRACT(data_type, r'(?:NUMERIC|BIGNUMERIC)\((\d+)') AS INT64) AS numeric_precision,
                CAST(REGEXP_EXTRACT(data_type, r'(?:NUMERIC|BIGNUMERIC)\(\d+,\s*(\d+)\)') AS INT64) AS numeric_scale
            FROM `{datasetId}`.INFORMATION_SCHEMA.COLUMNS
            """);

            var whereClauses = new List<string>();
            var parameters = new List<BigQueryParameter>();

            // [0] = catalog (project), [1] = schema (dataset), [2] = table, [3] = column
            if (restrictionValues?.Length > 0 && !string.IsNullOrWhiteSpace(restrictionValues[0]))
            {
                whereClauses.Add("table_catalog = @catalog");
                parameters.Add(new BigQueryParameter("@catalog", BigQueryDbType.String, restrictionValues[0]));
            }
            if (restrictionValues?.Length > 1 && !string.IsNullOrWhiteSpace(restrictionValues[1]))
            {
                whereClauses.Add("table_schema = @schema");
                parameters.Add(new BigQueryParameter("@schema", BigQueryDbType.String, restrictionValues[1]));
            }
            if (restrictionValues?.Length > 2 && !string.IsNullOrWhiteSpace(restrictionValues[2]))
            {
                whereClauses.Add("table_name = @tableName");
                parameters.Add(new BigQueryParameter("@tableName", BigQueryDbType.String, restrictionValues[2]));
            }
            if (restrictionValues?.Length > 3 && !string.IsNullOrWhiteSpace(restrictionValues[3]))
            {
                whereClauses.Add("column_name = @columnName");
                parameters.Add(new BigQueryParameter("@columnName", BigQueryDbType.String, restrictionValues[3]));
            }

            if (whereClauses.Count != 0)
            {
                queryBuilder.Append(" WHERE ");
                queryBuilder.Append(string.Join(" AND ", whereClauses));
            }

            queryBuilder.Append(" ORDER BY table_schema, table_name, ordinal_position");

            var resultTable = new DataTable("Columns");
            resultTable.Columns.Add("TABLE_CATALOG", typeof(string));
            resultTable.Columns.Add("TABLE_SCHEMA", typeof(string));
            resultTable.Columns.Add("TABLE_NAME", typeof(string));
            resultTable.Columns.Add("COLUMN_NAME", typeof(string));
            resultTable.Columns.Add("ORDINAL_POSITION", typeof(int));
            resultTable.Columns.Add("COLUMN_DEFAULT", typeof(string));
            resultTable.Columns.Add("IS_NULLABLE", typeof(bool));
            resultTable.Columns.Add("DATA_TYPE", typeof(string));
            resultTable.Columns.Add("CHARACTER_MAXIMUM_LENGTH", typeof(long));
            resultTable.Columns.Add("NUMERIC_PRECISION", typeof(int));
            resultTable.Columns.Add("NUMERIC_SCALE", typeof(int));

            using (var command = (BigQueryCommand)this.CreateCommand())
            {
                command.CommandText = queryBuilder.ToString();
                foreach (var p in parameters)
                {
                    command.Parameters.Add(p);
                }

                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var newRow = resultTable.NewRow();

                        newRow["TABLE_CATALOG"] = reader.IsDBNull(0) ? DBNull.Value : reader.GetString(0);
                        newRow["TABLE_SCHEMA"] = reader.IsDBNull(1) ? DBNull.Value : reader.GetString(1);
                        newRow["TABLE_NAME"] = reader.IsDBNull(2) ? DBNull.Value : reader.GetString(2);
                        newRow["COLUMN_NAME"] = reader.IsDBNull(3) ? DBNull.Value : reader.GetString(3);

                        if (!reader.IsDBNull(4)) newRow["ORDINAL_POSITION"] = Convert.ToInt32(reader.GetInt64(4));
                        else newRow["ORDINAL_POSITION"] = DBNull.Value;

                        newRow["COLUMN_DEFAULT"] = reader.IsDBNull(5) ? DBNull.Value : reader.GetString(5);

                        if (!reader.IsDBNull(6)) newRow["IS_NULLABLE"] = string.Equals(reader.GetString(6), "YES", StringComparison.OrdinalIgnoreCase);
                        else newRow["IS_NULLABLE"] = DBNull.Value;

                        newRow["DATA_TYPE"] = reader.IsDBNull(7) ? DBNull.Value : reader.GetString(7);

                        if (!reader.IsDBNull(8)) newRow["CHARACTER_MAXIMUM_LENGTH"] = reader.GetInt64(8);
                        else newRow["CHARACTER_MAXIMUM_LENGTH"] = DBNull.Value;

                        if (!reader.IsDBNull(9)) newRow["NUMERIC_PRECISION"] = Convert.ToInt32(reader.GetInt64(9));
                        else newRow["NUMERIC_PRECISION"] = DBNull.Value;

                        if (!reader.IsDBNull(10)) newRow["NUMERIC_SCALE"] = Convert.ToInt32(reader.GetInt64(10));
                        else newRow["NUMERIC_SCALE"] = DBNull.Value;

                        resultTable.Rows.Add(newRow);
                    }
                }
            }

            return resultTable;
        }

        private static DataTable GetRestrictionsSchema()
        {
            var restrictionsTable = new DataTable("Restrictions");

            restrictionsTable.Columns.Add("CollectionName", typeof(string));
            restrictionsTable.Columns.Add("RestrictionName", typeof(string));
            restrictionsTable.Columns.Add("ParameterName", typeof(string)); 
            restrictionsTable.Columns.Add("RestrictionDefault", typeof(string));
            restrictionsTable.Columns.Add("RestrictionNumber", typeof(int));

            Action<string, string, int> addRow = (collection, name, number) =>
            {
                restrictionsTable.Rows.Add(collection, name, "@" + name.ToLowerInvariant(), null, number);
            };

            addRow("Tables", "Catalog", 1);
            addRow("Tables", "Schema", 2);
            addRow("Tables", "Table", 3);

            addRow("Columns", "Catalog", 1);
            addRow("Columns", "Schema", 2);
            addRow("Columns", "Table", 3);
            addRow("Columns", "Column", 4);

            return restrictionsTable;
        }

        private void ParseConnectionString()
        {
            _parsedConnectionString.Clear();
            _projectId = null;
            _defaultDatasetId = null;
            _location = null;
            _credentialPath = null;
            _credentialJson = null;
            _useAdc = false;
            _connectionTimeoutSeconds = 15;

            if (string.IsNullOrWhiteSpace(_connectionString))
            {
                return;
            }

            var pairs = _connectionString.Split([';'], StringSplitOptions.RemoveEmptyEntries);
            foreach (var pair in pairs)
            {
                var kv = pair.Split(['='], 2);
                if (kv.Length == 2)
                {
                    _parsedConnectionString[kv[0].Trim()] = kv[1].Trim();
                }
            }

            _projectId = _parsedConnectionString.GetValueOrDefault("ProjectId");
            _defaultDatasetId = _parsedConnectionString.GetValueOrDefault("DefaultDatasetId");
            _location = _parsedConnectionString.GetValueOrDefault("Location");
            _credentialPath = _parsedConnectionString.GetValueOrDefault("CredentialsFile");
            _credentialJson = _parsedConnectionString.GetValueOrDefault("JsonCredentials");

            string authMethod = _parsedConnectionString.GetValueOrDefault("AuthMethod");
            _useAdc = string.Equals(authMethod, "ApplicationDefaultCredentials", StringComparison.OrdinalIgnoreCase)
                || string.IsNullOrWhiteSpace(authMethod); // Default to ADC if not specified

            var dataSource = _parsedConnectionString.GetValueOrDefault("DataSource");
            if (!string.IsNullOrWhiteSpace(dataSource))
            {
                _dataSource = dataSource.Trim();
            }

            if (_parsedConnectionString.TryGetValue("Timeout", out var timeoutStr) && int.TryParse(timeoutStr, out var timeoutVal) && timeoutVal >= 0)
            {
                _connectionTimeoutSeconds = timeoutVal;
            }

            if (string.IsNullOrWhiteSpace(_projectId))
            {
                throw new ArgumentException("ProjectId must be specified in the connection string.");
            }
        }

        private ConnectionState SetState(ConnectionState newState)
        {
            if (_state == newState) return _state;

            var previousState = _state;
            _state = newState;
            OnStateChange(new StateChangeEventArgs(previousState, newState));
            return previousState;
        }

        protected virtual void OnStateChange(StateChangeEventArgs args)
        {
            StateChange?.Invoke(this, args);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (State != ConnectionState.Closed)
                {
                    Close();
                }
            }

            _isDisposed = true;
            base.Dispose(disposing);
        }

        private void EnsureConnectionOpen()
        {
            if (State != ConnectionState.Open)
            {
                throw new InvalidOperationException("Connection must be open to perform this operation.");
            }
        }

        private void VerifyNotDisposed()
        {
            ObjectDisposedException.ThrowIf(_isDisposed, this);
        }
    }

    internal static class DictionaryExtensions
    {
        public static TValue GetValueOrDefault<TKey, TValue>(this IDictionary<TKey, TValue> dictionary, TKey key)
        {
            return dictionary.TryGetValue(key, out var value) ? value : default;
        }
    }
}

