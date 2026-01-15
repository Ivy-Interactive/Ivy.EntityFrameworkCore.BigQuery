using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Storage;

namespace Ivy.EntityFrameworkCore.BigQuery.Migrations
{
    /// <summary>
    /// BigQuery implementation of <see cref="IHistoryRepository"/>.
    /// </summary>
    /// <remarks>
    /// BigQuery does not support traditional database locking mechanisms (advisory locks, SELECT FOR UPDATE, etc.).
    /// This implementation uses no-op locking and relies on:
    /// 1. The migration history table to prevent re-running applied migrations
    /// 2. Idempotent DDL operations (CREATE TABLE IF NOT EXISTS, etc.)
    /// 3. Single-deployer patterns typical in BigQuery environments
    ///
    /// Concurrent migrations are not supported. Run migrations from a single process.
    /// </remarks>
    public class BigQueryHistoryRepository : HistoryRepository
    {
        public BigQueryHistoryRepository(HistoryRepositoryDependencies dependencies)
            : base(dependencies)
        {
        }

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

        /// <inheritdoc />
        public override IMigrationsDatabaseLock AcquireDatabaseLock()
        {
            Dependencies.MigrationsLogger.AcquiringMigrationLock();
            return new BigQueryMigrationDatabaseLock(this);
        }

        /// <inheritdoc />
        public override Task<IMigrationsDatabaseLock> AcquireDatabaseLockAsync(CancellationToken cancellationToken = default)
        {
            Dependencies.MigrationsLogger.AcquiringMigrationLock();
            return Task.FromResult<IMigrationsDatabaseLock>(new BigQueryMigrationDatabaseLock(this));
        }

        /// <summary>
        /// A no-op migration lock for BigQuery.
        /// BigQuery doesn't support database-level locking, so this implementation
        /// relies on the migration history table for coordination.
        /// </summary>
        private sealed class BigQueryMigrationDatabaseLock : IMigrationsDatabaseLock
        {
            public BigQueryMigrationDatabaseLock(IHistoryRepository historyRepository)
            {
                HistoryRepository = historyRepository;
            }

            public IHistoryRepository HistoryRepository { get; }

            public void Dispose()
            {
            }

            public ValueTask DisposeAsync() => ValueTask.CompletedTask;
        }
    }
}
