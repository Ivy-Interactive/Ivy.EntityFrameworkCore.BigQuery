using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Storage;


namespace Ivy.EntityFrameworkCore.BigQuery.Migrations
{
    public class BigQueryHistoryRepository : HistoryRepository
    {
        private static readonly TimeSpan RetryDelay = TimeSpan.FromSeconds(10);
        private const int MaxRetries = 10;

        public BigQueryHistoryRepository(HistoryRepositoryDependencies dependencies)
            : base(dependencies)
        {
        }

        protected virtual string LockTableName { get; } = "__EFMigrationsLock";

        /// <inheritdoc />
        public override LockReleaseBehavior LockReleaseBehavior => LockReleaseBehavior.Explicit;

        /// <inheritdoc />
        protected override string ExistsSql
        {
            get
            {
                var stringTypeMapping = Dependencies.TypeMappingSource.GetMapping(typeof(string));
                var tableNameLiteral = stringTypeMapping.GenerateSqlLiteral(TableName);
                var dataset = SqlGenerationHelper.DelimitIdentifier(Dependencies.Connection.DbConnection.Database);
                var datasetLiteral = stringTypeMapping.GenerateSqlLiteral(Dependencies.Connection.DbConnection.Database);

                return $"""
SELECT COUNT(1)
FROM {dataset}.INFORMATION_SCHEMA.TABLES
WHERE table_schema = {datasetLiteral}
AND table_name = {tableNameLiteral}
""";
            }
        }

        /// <inheritdoc />
        protected override bool InterpretExistsResult(object? value)
        {
            return value != null && Convert.ToInt64(value) == 1;
        }

        /// <inheritdoc />
        public override string GetCreateIfNotExistsScript()
        {
            var script = GetCreateScript();

            return script.Replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS");
        }

        /// <inheritdoc />
        public override string GetBeginIfNotExistsScript(string migrationId)
        {
            var stringTypeMapping = Dependencies.TypeMappingSource.GetMapping(typeof(string));

            return $"""
BEGIN
IF NOT EXISTS(SELECT 1 FROM {SqlGenerationHelper.DelimitIdentifier(TableName, TableSchema)} WHERE {SqlGenerationHelper.DelimitIdentifier(MigrationIdColumnName)} = {stringTypeMapping.GenerateSqlLiteral(migrationId)}) THEN
""";
        }

        /// <inheritdoc />
        public override string GetBeginIfExistsScript(string migrationId)
        {
            var stringTypeMapping = Dependencies.TypeMappingSource.GetMapping(typeof(string));

            return $"""
BEGIN
IF EXISTS(SELECT 1 FROM {SqlGenerationHelper.DelimitIdentifier(TableName, TableSchema)} WHERE {SqlGenerationHelper.DelimitIdentifier(MigrationIdColumnName)} = {stringTypeMapping.GenerateSqlLiteral(migrationId)}) THEN
""";
        }

        /// <inheritdoc />
        public override string GetEndIfScript()
        {
            return $"""
END IF;
END;
""";
        }

        public override IMigrationsDatabaseLock AcquireDatabaseLock()
        {
            Dependencies.MigrationsLogger.AcquiringMigrationLock();

            EnsureLockTableExists();

            var lockId = Guid.NewGuid().ToString();
            var retryCount = 0;
            var retryDelay = RetryDelay;

            while (retryCount < MaxRetries)
            {
                try
                {
                    var lockAcquired = TryAcquireLock(lockId);
                    if (lockAcquired)
                    {
                        return new BigQueryMigrationDatabaseLock(lockId, this);
                    }

                    Thread.Sleep(retryDelay);
                    retryCount++;

                    retryDelay = TimeSpan.FromMilliseconds(
                        retryDelay.TotalMilliseconds * 1.5 + Random.Shared.Next(0, 1000));
                }
                catch (Exception ex) when (retryCount < MaxRetries - 1)
                {
                    Thread.Sleep(retryDelay);
                    retryCount++;
                    retryDelay = TimeSpan.FromMilliseconds(retryDelay.TotalMilliseconds * 1.5);
                }
            }

            throw new TimeoutException("Unable to acquire migration lock after maximum retries.");
        }

        public override async Task<IMigrationsDatabaseLock> AcquireDatabaseLockAsync(CancellationToken cancellationToken = default)
        {
            Dependencies.MigrationsLogger.AcquiringMigrationLock();

            await EnsureLockTableExistsAsync(cancellationToken);

            var lockId = Guid.NewGuid().ToString();
            var retryCount = 0;
            var retryDelay = RetryDelay;

            while (retryCount < MaxRetries)
            {
                try
                {
                    var lockAcquired = await TryAcquireLockAsync(lockId, cancellationToken);
                    if (lockAcquired)
                    {
                        return new BigQueryMigrationDatabaseLock(lockId, this);
                    }

                    await Task.Delay(retryDelay, cancellationToken);
                    retryCount++;

                    retryDelay = TimeSpan.FromMilliseconds(
                        retryDelay.TotalMilliseconds * 1.5 + Random.Shared.Next(0, 1000));
                }
                catch (Exception ex) when (retryCount < MaxRetries - 1)
                {
                    await Task.Delay(retryDelay, cancellationToken);
                    retryCount++;
                    retryDelay = TimeSpan.FromMilliseconds(retryDelay.TotalMilliseconds * 1.5);
                }
            }

            throw new TimeoutException("Unable to acquire migration lock after maximum retries.");
        }

        private void EnsureLockTableExists()
        {
            var lockTableName = GetLockTableName();
            var existsCommand = Dependencies.RawSqlCommandBuilder.Build(CreateLockTableExistsSql(lockTableName));

            var exists = InterpretExistsResult(existsCommand.ExecuteScalar(CreateRelationalCommandParameters()));
            if (!exists)
            {
                var createCommand = CreateLockTableCommand(lockTableName);
                createCommand.ExecuteNonQuery(CreateRelationalCommandParameters());
            }
        }

        private async Task EnsureLockTableExistsAsync(CancellationToken cancellationToken)
        {
            var lockTableName = GetLockTableName();
            var existsCommand = Dependencies.RawSqlCommandBuilder.Build(CreateLockTableExistsSql(lockTableName));

            var exists = InterpretExistsResult(
                await existsCommand.ExecuteScalarAsync(CreateRelationalCommandParameters(), cancellationToken));

            if (!exists)
            {
                var createCommand = CreateLockTableCommand(lockTableName);
                await createCommand.ExecuteNonQueryAsync(CreateRelationalCommandParameters(), cancellationToken);
            }
        }

        private bool TryAcquireLock(string lockId)
        {
            var lockTableName = GetLockTableName();
            var command = CreateAcquireLockCommand(lockTableName, lockId);

            try
            {
                var result = command.ExecuteScalar(CreateRelationalCommandParameters());
                return result switch
                {
                    null => false,
                    long longValue => longValue > 0,
                    int intValue => intValue > 0,
                    decimal decimalValue => decimalValue > 0,
                    double doubleValue => doubleValue > 0,
                    _ => Convert.ToInt64((object)result) > 0
                };
            }
            catch
            {
                return false;
            }
        }

        private async Task<bool> TryAcquireLockAsync(string lockId, CancellationToken cancellationToken)
        {
            var lockTableName = GetLockTableName();
            var command = CreateAcquireLockCommand(lockTableName, lockId);

            try
            {
                var result = await command.ExecuteScalarAsync(CreateRelationalCommandParameters(), cancellationToken);
                return result switch
                {
                    null => false,
                    long longValue => longValue > 0,
                    int intValue => intValue > 0,
                    decimal decimalValue => decimalValue > 0,
                    double doubleValue => doubleValue > 0,
                    _ => Convert.ToInt64((object)result) > 0
                };
            }
            catch
            {
                return false;
            }
        }

        internal void ReleaseLock(string lockId)
        {
            try
            {
                var lockTableName = GetLockTableName();
                var command = CreateReleaseLockCommand(lockTableName, lockId);
                command.ExecuteNonQuery(CreateRelationalCommandParameters());
            }
            catch
            {
                // Ignore
            }
        }

        internal async Task ReleaseLockAsync(string lockId)
        {
            try
            {
                var lockTableName = GetLockTableName();
                var command = CreateReleaseLockCommand(lockTableName, lockId);
                await command.ExecuteNonQueryAsync(CreateRelationalCommandParameters(), CancellationToken.None);
            }
            catch
            {
                // Ignore
            }
        }

        private string GetLockTableName()
            => $"{TableName}_Lock";

        private string CreateLockTableExistsSql(string lockTableName)
        {
            var schema = TableSchema ?? Dependencies.Connection.DbConnection.Database;
            var stringTypeMapping = Dependencies.TypeMappingSource.GetMapping(typeof(string));
            return $"""
SELECT COUNT(*) 
FROM {SqlGenerationHelper.DelimitIdentifier(schema)}.INFORMATION_SCHEMA.TABLES 
WHERE table_name = {stringTypeMapping.GenerateSqlLiteral(lockTableName)}
""";
        }

        private IRelationalCommand CreateLockTableCommand(string lockTableName)
        {
            var tableName = SqlGenerationHelper.DelimitIdentifier(lockTableName, TableSchema);
            var sql = $"""
CREATE TABLE IF NOT EXISTS {tableName} (
    LockId STRING NOT NULL,
    AcquiredAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP() NOT NULL,
    ExpiresAt TIMESTAMP NOT NULL
)
""";
            return Dependencies.RawSqlCommandBuilder.Build(sql);
        }

        private IRelationalCommand CreateAcquireLockCommand(string lockTableName, string lockId)
        {
            var stringTypeMapping = Dependencies.TypeMappingSource.GetMapping(typeof(string));
            var lockIdLiteral = stringTypeMapping.GenerateSqlLiteral(lockId);
            var tableName = SqlGenerationHelper.DelimitIdentifier(lockTableName, TableSchema);

            var sql = $"""
BEGIN
  DECLARE lock_count INT64;

  DELETE FROM {tableName}
  WHERE ExpiresAt < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE);

  SET lock_count = (SELECT COUNT(*) FROM {tableName});

  IF lock_count = 0 THEN
    INSERT INTO {tableName} (LockId, AcquiredAt, ExpiresAt)
    VALUES ({lockIdLiteral}, CURRENT_TIMESTAMP(), TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE));
  END IF;

  SELECT @@row_count;
END;
""";
            return Dependencies.RawSqlCommandBuilder.Build(sql);
        }

        private RelationalCommandParameterObject CreateRelationalCommandParameters()
            => new(
                Dependencies.Connection,
                null,
                null,
                Dependencies.CurrentContext.Context,
                Dependencies.CommandLogger,
                CommandSource.Migrations);

        private IRelationalCommand CreateReleaseLockCommand(string lockTableName, string lockId)
        {
            var stringTypeMapping = Dependencies.TypeMappingSource.GetMapping(typeof(string));
            var lockIdLiteral = stringTypeMapping.GenerateSqlLiteral(lockId);
            var tableName = SqlGenerationHelper.DelimitIdentifier(lockTableName, TableSchema);

            var sql = $"""
DELETE FROM {tableName}
WHERE LockId = {lockIdLiteral}
""";
            return Dependencies.RawSqlCommandBuilder.Build(sql);
        }

        private sealed class BigQueryMigrationDatabaseLock : IMigrationsDatabaseLock
        {
            private readonly string _lockId;
            private readonly BigQueryHistoryRepository _repository;
            private bool _disposed;

            public BigQueryMigrationDatabaseLock(string lockId, BigQueryHistoryRepository repository)
            {
                _lockId = lockId;
                _repository = repository;
            }

            public IHistoryRepository HistoryRepository => _repository;

            public void Dispose()
            {
                if (!_disposed)
                {
                    _repository.ReleaseLock(_lockId);
                    _disposed = true;
                }
            }

            public async ValueTask DisposeAsync()
            {
                if (!_disposed)
                {
                    await _repository.ReleaseLockAsync(_lockId);
                    _disposed = true;
                }
            }
        }
    }
}