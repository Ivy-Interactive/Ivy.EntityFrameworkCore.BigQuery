using Google.Cloud.BigQuery.V2;
using System.Data.Common;
using System.Data;
using Google.Apis.Bigquery.v2.Data;
using System.Diagnostics;

namespace Ivy.Data.BigQuery
{
    public class BigQueryCommand : DbCommand
    {
        private DbTransaction _transaction;
        private string _commandText = string.Empty;
        private int _commandTimeout = 30;
        private CommandType _commandType = CommandType.Text;
        private BigQueryParameterCollection _parameters;
        private bool _designTimeVisible = false;
        private UpdateRowSource _updatedRowSource = UpdateRowSource.None;
        private CancellationTokenSource _cancellationTokenSource;
        private BigQueryConnection _connection;

        public BigQueryCommand()
        {
        }

        public BigQueryCommand(string commandText) : this()
        {
            CommandText = commandText;
        }

        public BigQueryCommand(string commandText, BigQueryConnection connection) : this(commandText)
        {
            Connection = connection;
        }

        public BigQueryCommand(string commandText, BigQueryConnection connection, DbTransaction transaction) : this(commandText, connection)
        {
            Transaction = transaction;
        }

        public override string CommandText
        {
            get => _commandText;
            set
            {
                if (DataReader != null)
                {
                    throw new InvalidOperationException($"An open reader is associated with this command. Close it before changing the {nameof(CommandText)} property.");
                }
                _commandText = value ?? string.Empty;
            }
        }

        protected internal virtual BigQueryDataReader? DataReader { get; set; }

        public override int CommandTimeout
        {
            get => _commandTimeout;
            set
            {
                if (value < 0) throw new ArgumentOutOfRangeException(nameof(value), "CommandTimeout must be 0 or greater.");
                _commandTimeout = value;
            }
        }

        public override CommandType CommandType
        {
            get => _commandType;
            set
            {
                if (value != CommandType.Text)
                {
                    throw new NotSupportedException("Only CommandType.Text is supported by BigQueryCommand.");
                }
                _commandType = value;
            }
        }

        public new virtual BigQueryConnection Connection
        {
            get => _connection;
            set
            {
                if (DataReader != null)
                {
                    throw new InvalidOperationException($"An open reader is associated with this command. Close it before changing the {nameof(Connection)} property.");
                }
                _connection = value;
            }
        }

        protected override DbConnection DbConnection
        {
            get => Connection;
            set
            {
                Connection = (BigQueryConnection?)value;
            }
        }

        public new BigQueryParameterCollection Parameters => _parameters ??= [];

        protected override DbParameterCollection DbParameterCollection => Parameters;

        public new DbTransaction Transaction
        {
            get => _transaction;
            set
            {
                if (_transaction != null && _transaction != value)
                {
                }
                _transaction = value;
            }
        }

        protected override DbTransaction DbTransaction
        {
            get => Transaction;
            set => Transaction = value;
        }

        public override bool DesignTimeVisible { get => _designTimeVisible; set => _designTimeVisible = value; }
        public override UpdateRowSource UpdatedRowSource { get => _updatedRowSource; set => _updatedRowSource = value; }

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            try
            {
                return ExecuteDbDataReaderAsync(behavior, CancellationToken.None).GetAwaiter().GetResult();
            }
            catch (AggregateException ae) when (ae.InnerExceptions.Count == 1)
            {
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(ae.InnerException).Throw();
                throw;
            }
        }

        protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (DataReader != null)
            {
                throw new InvalidOperationException("An open reader is already associated with this command. Close it before opening a new one.");
            }

            var client = GetClientAndCheckState();
            LogCommand();
            var queryOptions = CreateQueryOptions();
            var bqParameters = Parameters.ToBigQueryParameters(CommandText);

            CancellationTokenSource internalCts;
            lock (this)
            {
                if (_cancellationTokenSource == null || _cancellationTokenSource.IsCancellationRequested)
                {
                    _cancellationTokenSource?.Dispose();
                    _cancellationTokenSource = new CancellationTokenSource();
                }
                internalCts = _cancellationTokenSource;
            }
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(internalCts.Token, cancellationToken);
            var effectiveToken = linkedCts.Token;

            try
            {
                effectiveToken.ThrowIfCancellationRequested();

                var job = await client.CreateQueryJobAsync(
                    sql: CommandText,
                    parameters: bqParameters,
                    options: queryOptions,
                    cancellationToken: effectiveToken).ConfigureAwait(false);

                effectiveToken.ThrowIfCancellationRequested();

                job = await job.PollUntilCompletedAsync(cancellationToken: effectiveToken).ConfigureAwait(false);

                effectiveToken.ThrowIfCancellationRequested();

                if (job.Status.ErrorResult != null)
                {
                    throw BigQueryExceptionHelper.CreateException(job.Status.ErrorResult, $"Query execution failed: {job.Status.ErrorResult.Message}{Environment.NewLine}{this.CommandText}");
                }

                var results = await client.GetQueryResultsAsync(
                    jobReference: job.Reference,
                    options: null,
                    cancellationToken: effectiveToken).ConfigureAwait(false);

                var recordsAffected = results.NumDmlAffectedRows.HasValue ? (int)results.NumDmlAffectedRows.Value : -1;

                var closeConnection = behavior.HasFlag(CommandBehavior.CloseConnection);
                var dataReader = new BigQueryDataReader(client, results, this, behavior, closeConnection, recordsAffected);
                return DataReader = dataReader;

            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Google.GoogleApiException gax)
            {
                effectiveToken.ThrowIfCancellationRequested();
                throw BigQueryExceptionHelper.CreateException(gax, "BigQuery API request failed during query execution.");
            }
            catch (Exception ex) when (ex is not (OperationCanceledException or DbException))
            {
                throw new BigQueryException($"An error occurred while executing the BigQuery command: {ex.Message}", ex);
            }
        }

        public override int ExecuteNonQuery()
        {
            try
            {
                return ExecuteNonQueryAsync(CancellationToken.None).GetAwaiter().GetResult();
            }
            catch (AggregateException ae) when (ae.InnerExceptions.Count == 1)
            {
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(ae.InnerException).Throw();
                throw;
            }
        }

        public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var client = GetClientAndCheckState();
            LogCommand();
            var queryOptions = CreateQueryOptions();
            var bqParameters = Parameters.ToBigQueryParameters(CommandText);

            CancellationTokenSource internalCts;
            lock (this) 
            {
                if (_cancellationTokenSource == null || _cancellationTokenSource.IsCancellationRequested)
                {
                    _cancellationTokenSource?.Dispose();
                    _cancellationTokenSource = new CancellationTokenSource();
                }
                internalCts = _cancellationTokenSource;
            }
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(internalCts.Token, cancellationToken);
            var effectiveToken = linkedCts.Token;

            try
            {
                effectiveToken.ThrowIfCancellationRequested();

                var job = await client.CreateQueryJobAsync(
                    sql: CommandText,
                    parameters: bqParameters,
                    options: queryOptions,
                    cancellationToken: effectiveToken).ConfigureAwait(false);

                effectiveToken.ThrowIfCancellationRequested();

                job = await job.PollUntilCompletedAsync(cancellationToken: effectiveToken).ConfigureAwait(false);

                effectiveToken.ThrowIfCancellationRequested();

                if (job.Status.ErrorResult != null)
                {
                    throw BigQueryExceptionHelper.CreateException(job.Status.ErrorResult, $"Query execution failed: {job.Status.ErrorResult.Message}");
                }

                var results = await client.GetQueryResultsAsync(
                    jobReference: job.Reference,
                    options: null,
                    cancellationToken: effectiveToken).ConfigureAwait(false);

                return (int?)results.NumDmlAffectedRows ?? -1;

            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Google.GoogleApiException gax)
            {
                throw BigQueryExceptionHelper.CreateException(gax, "BigQuery API request failed during command execution.");
            }
            catch (Exception ex) when (ex is not (OperationCanceledException or DbException))
            {
                effectiveToken.ThrowIfCancellationRequested();
                throw new BigQueryException($"An error occurred while executing the BigQuery command: {ex.Message}", ex);
            }
        }


        public override object ExecuteScalar()
        {
            try
            {
                return ExecuteScalarAsync(CancellationToken.None).GetAwaiter().GetResult();
            }
            catch (AggregateException ae) when (ae.InnerExceptions.Count == 1)
            {
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(ae.InnerException).Throw();
                throw;
            }
        }

        public override async Task<object> ExecuteScalarAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            await using var reader = await ExecuteDbDataReaderAsync(CommandBehavior.Default, cancellationToken).ConfigureAwait(false);

            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false)) return null;

                return reader.FieldCount > 0 ?
                    reader.GetValue(0) :
                    null;
            }
            finally
            {
            }
        }

        public override void Cancel()
        {
            lock (this)
            {
                if (_cancellationTokenSource is { IsCancellationRequested: false })
                {
                    _cancellationTokenSource.Cancel();
                }
            }
        }

        protected override DbParameter CreateDbParameter()
        {
            return new BigQueryParameter();
        }

        public override void Prepare()
        {
            if (Connection is not { State: ConnectionState.Open })
            {
                throw new InvalidOperationException("Connection must be valid and open to prepare the command.");
            }
            if (string.IsNullOrWhiteSpace(CommandText))
            {
                throw new InvalidOperationException("CommandText must be set before preparing.");
            }
        }

        private BigQueryClient GetClientAndCheckState()
        {
            if (Connection == null)
            {
                throw new InvalidOperationException("Connection property must be initialized.");
            }
            if (Connection.State != ConnectionState.Open)
            {
                throw new InvalidOperationException("Connection must be open to execute commands.");
            }
            if (string.IsNullOrWhiteSpace(CommandText))
            {
                throw new InvalidOperationException("CommandText property must be set.");
            }

            var client = Connection.Client;
            if (client == null)
            {
                throw new InvalidOperationException("The underlying BigQueryClient is not available on the connection.");
            }
            return client;
        }

        private QueryOptions CreateQueryOptions()
        {
            var options = new QueryOptions();

            if (Connection != null && !string.IsNullOrEmpty(Connection.DefaultProjectId) && !string.IsNullOrEmpty(Connection.DefaultDatasetId))
            {
                options.DefaultDataset = new DatasetReference
                {
                    ProjectId = Connection.DefaultProjectId,
                    DatasetId = Connection.DefaultDatasetId
                };
            }

            return options;
        }

        [Conditional("DEBUG")]
        private void LogCommand()
        {
            Debug.WriteLine("--- Executing BigQuery Command ---");
            Debug.WriteLine($"CommandText: {CommandText}");
            if (Parameters.Count > 0)
            {
                Debug.WriteLine("Parameters:");
                foreach (BigQueryParameter p in Parameters)
                {
                    string valueStr = p.Value == null ? "NULL" : p.Value == DBNull.Value ? "DBNull" : p.Value.ToString();
                    string typeStr = p.BigQueryDbType.HasValue ? p.BigQueryDbType.Value.ToString() : p.DbType.ToString() ?? "Unknown";
                    Debug.WriteLine($"  {p.ParameterName} ({typeStr}) = {valueStr}");
                }
            }

            Debug.WriteLine("----------------------------------");
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {

                _transaction = null;
                _connection = null;
                _parameters?.Clear();
                
                lock (this)
                {
                    _cancellationTokenSource?.Dispose();
                    _cancellationTokenSource = null;
                }
            }

            base.Dispose(disposing);
        }
    }
}