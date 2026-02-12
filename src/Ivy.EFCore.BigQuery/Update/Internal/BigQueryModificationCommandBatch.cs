using Ivy.EntityFrameworkCore.BigQuery.Storage.Internal.Mapping;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Update;

namespace Ivy.EntityFrameworkCore.BigQuery.Update.Internal
{

    public class BigQueryModificationCommandBatch : AffectedCountModificationCommandBatch
    {
        private const int DefaultMaxBatchSize = 1000;
        private const int MaxParameterCount = 10000;

        // BigQuery has a 10MB request size limit
        // https://cloud.google.com/bigquery/quotas#standard_tables
        private const int MaxRequestSizeBytes = 10_000_000;
        // Use 9.5MB safety margin to account for HTTP headers, encoding overhead, and parameter values
        private const int MaxRequestSizeSafetyMargin = 9_500_000;

        private readonly List<IReadOnlyModificationCommand> _pendingBulkInsertCommands = [];

        public BigQueryModificationCommandBatch(
            ModificationCommandBatchFactoryDependencies dependencies)
            : base(dependencies, DefaultMaxBatchSize)
        {
        }

        protected new virtual IBigQueryUpdateSqlGenerator UpdateSqlGenerator
            => (IBigQueryUpdateSqlGenerator)base.UpdateSqlGenerator;


        protected override bool IsValid()
        {
            if (ParameterValues.Count > MaxParameterCount)
            {
                return false;
            }

            var estimatedSizeBytes = EstimateSqlSizeInBytes();
            if (estimatedSizeBytes > MaxRequestSizeSafetyMargin)
            {
                return false;
            }

            return base.IsValid();
        }

        private int EstimateSqlSizeInBytes()
        {
            var sqlLength = SqlBuilder.Length;

            if (_pendingBulkInsertCommands.Count > 0)
            {
                var firstCommand = _pendingBulkInsertCommands[0];
                var numColumns = firstCommand.ColumnModifications.Count(o => o.IsWrite);

                // Column identifiers: numColumns * 50 bytes average
                // Parameter placeholders or literals: numColumns * 100 bytes average
                // Parentheses, commas, and whitespace: numColumns * 2 bytes
                var estimatedBytesPerRow = numColumns * 150 + 10;
                sqlLength += _pendingBulkInsertCommands.Count * estimatedBytesPerRow;

                // INSERT statement overhead
                sqlLength += 200;
            }

            // UTF-8 encoding
            return sqlLength * 3;
        }

        protected override void RollbackLastCommand(IReadOnlyModificationCommand modificationCommand)
        {
            if (_pendingBulkInsertCommands.Count > 0)
            {
                _pendingBulkInsertCommands.RemoveAt(_pendingBulkInsertCommands.Count - 1);
            }

            base.RollbackLastCommand(modificationCommand);
        }

        private void ApplyPendingBulkInsertCommands()
        {
            if (_pendingBulkInsertCommands.Count == 0)
            {
                return;
            }

            var commandPosition = ResultSetMappings.Count;
            var wasCachedCommandTextEmpty = IsCommandTextEmpty;

            var resultSetMapping = UpdateSqlGenerator.AppendBulkInsertOperation(
                SqlBuilder, _pendingBulkInsertCommands, commandPosition, out var requiresTransaction);

            SetRequiresTransaction(!wasCachedCommandTextEmpty || requiresTransaction);

            for (var i = 0; i < _pendingBulkInsertCommands.Count; i++)
            {
                ResultSetMappings.Add(resultSetMapping);
            }

            if (resultSetMapping.HasFlag(ResultSetMapping.HasResultRow))
            {
                ResultSetMappings[^1] &= ~ResultSetMapping.NotLastInResultSet;
                ResultSetMappings[^1] |= ResultSetMapping.LastInResultSet;
            }
        }

        public override bool TryAddCommand(IReadOnlyModificationCommand modificationCommand)
        {
            if (_pendingBulkInsertCommands.Count > 0
                && (modificationCommand.EntityState != EntityState.Added
                    || modificationCommand.StoreStoredProcedure is not null
                    || !CanBeInsertedInSameStatement(_pendingBulkInsertCommands[0], modificationCommand)))
            {
                ApplyPendingBulkInsertCommands();
                _pendingBulkInsertCommands.Clear();
            }

            return base.TryAddCommand(modificationCommand);
        }

        protected override void AddCommand(IReadOnlyModificationCommand modificationCommand)
        {
            if (modificationCommand is { EntityState: EntityState.Added, StoreStoredProcedure: null })
            {
                _pendingBulkInsertCommands.Add(modificationCommand);

                // Only add parameters if there are no STRUCT or GEOGRAPHY columns
                // (SQL generator will use literals for all columns when these types are present)
                var hasSpecialColumn = modificationCommand.ColumnModifications
                    .Any(c => c.TypeMapping is BigQueryStructTypeMapping
                              || IsGeographyTypeMapping(c.TypeMapping));

                if (!hasSpecialColumn)
                {
                    AddParameters(modificationCommand);
                }
            }
            else
            {
                base.AddCommand(modificationCommand);
            }
        }

        private static bool IsGeographyTypeMapping(Microsoft.EntityFrameworkCore.Storage.RelationalTypeMapping? typeMapping)
        {
            return typeMapping?.StoreType?.Equals("GEOGRAPHY", StringComparison.OrdinalIgnoreCase) == true;
        }

        private static bool CanBeInsertedInSameStatement(
            IReadOnlyModificationCommand firstCommand,
            IReadOnlyModificationCommand secondCommand)
            => firstCommand.TableName == secondCommand.TableName
                && firstCommand.Schema == secondCommand.Schema
                && firstCommand.ColumnModifications.Where(o => o.IsWrite).Select(o => o.ColumnName).SequenceEqual(
                    secondCommand.ColumnModifications.Where(o => o.IsWrite).Select(o => o.ColumnName))
                && firstCommand.ColumnModifications.Where(o => o.IsRead).Select(o => o.ColumnName).SequenceEqual(
                    secondCommand.ColumnModifications.Where(o => o.IsRead).Select(o => o.ColumnName));

        public override void Complete(bool moreBatchesExpected)
        {
            ApplyPendingBulkInsertCommands();
            base.Complete(moreBatchesExpected);
        }
    }
}