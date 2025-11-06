using Google;
using Ivy.Data.BigQuery;
using Microsoft.EntityFrameworkCore.Storage;
using System.Data.Common;
using Ivy.EntityFrameworkCore.BigQuery.Migrations.Operations;

namespace Ivy.EntityFrameworkCore.BigQuery.Storage.Internal
{
    public class BigQueryDatabaseCreator : RelationalDatabaseCreator
    {
        private readonly IBigQueryRelationalConnection _connection;
        private readonly IRawSqlCommandBuilder _rawSqlCommandBuilder;

        public BigQueryDatabaseCreator(
            RelationalDatabaseCreatorDependencies dependencies,
            IBigQueryRelationalConnection connection, IRawSqlCommandBuilder rawSqlCommandBuilder)
            : base(dependencies)
        {
            _connection = connection;
            _rawSqlCommandBuilder = rawSqlCommandBuilder;
        }

        /// <inheritdoc/>
        public override void Create()
        {
            var datasetId = GetRequiredDatasetId();
            var operations = new[] { new BigQueryCreateDatasetOperation { Name = datasetId } };
            var commands = Dependencies.MigrationsSqlGenerator.Generate(operations);

            Dependencies.MigrationCommandExecutor.ExecuteNonQuery(commands, _connection);
        }

        /// <inheritdoc/>
        public override async Task CreateAsync(CancellationToken cancellationToken = default)
        {
            var datasetId = GetRequiredDatasetId();
            var operations = new[] { new BigQueryCreateDatasetOperation { Name = datasetId } };
            var commands = Dependencies.MigrationsSqlGenerator.Generate(operations);

            await Dependencies.MigrationCommandExecutor.ExecuteNonQueryAsync(commands, _connection, cancellationToken: cancellationToken)
                .ConfigureAwait(false);
        }

        /// <inheritdoc/>
        public override void Delete()
        {
            var datasetId = GetRequiredDatasetId();
            var operations = new[] { new BigQueryDropDatasetOperation { Name = datasetId, Behavior = BigQueryDropDatasetOperation.BigQueryDropDatasetBehavior.Cascade } };
            var commands = Dependencies.MigrationsSqlGenerator.Generate(operations);

            for (var i = 0; i < 5; i++)
            {
                try
                {
                    Dependencies.MigrationCommandExecutor.ExecuteNonQuery(commands, _connection);
                    return;
                }
                catch (GoogleApiException ex) when (ex.Error?.Code == 400 && ex.Error.Message.Contains("dataset is still in use"))
                {
                    Thread.Sleep(1000);
                }
                catch (GoogleApiException ex) when (IsDoesNotExist(ex))
                {
                    return;
                }
            }
        }

        /// <inheritdoc/>
        public override async Task DeleteAsync(CancellationToken cancellationToken = default)
        {
            var datasetId = GetRequiredDatasetId();
            var operations = new[] { new BigQueryDropDatasetOperation { Name = datasetId, Behavior = BigQueryDropDatasetOperation.BigQueryDropDatasetBehavior.Cascade } };
            var commands = Dependencies.MigrationsSqlGenerator.Generate(operations);

            try
            {
                await Dependencies.MigrationCommandExecutor.ExecuteNonQueryAsync(commands, _connection, cancellationToken: cancellationToken)
                    .ConfigureAwait(false);
                return;
            }
            catch (GoogleApiException ex) when (ex.Error?.Code == 400 && ex.Error.Message.Contains("dataset is still in use"))
            {
                await Task.Delay(1000, cancellationToken);
            }
            catch (GoogleApiException ex) when (IsDoesNotExist(ex))
            {
                return;
            }

        }

        /// <inheritdoc/>
        public override bool Exists()
        {
            try
            {
                using var command = CreateExistsCommand();
                _connection.Open();
                var result = command.ExecuteScalar();
                return result != null && (long)result > 0;
            }
            catch (BigQueryException)
            {
                return false;
            }
            finally
            {
                if (_connection.DbConnection.State == System.Data.ConnectionState.Open)
                {
                    _connection.Close();
                }
            }
        }

        /// <inheritdoc/>
        public override async Task<bool> ExistsAsync(CancellationToken cancellationToken = default)
        {
            try
            {
                await using var command = CreateExistsCommand();
                await _connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                return result != null && (long)result > 0;
            }
            catch (BigQueryException)
            {
                return false;
            }
            finally
            {
                if (_connection.DbConnection.State == System.Data.ConnectionState.Open)
                {
                    await _connection.CloseAsync().ConfigureAwait(false);
                }
            }
        }

        /// <inheritdoc/>
        public override bool HasTables()
        {
            var datasetId = GetRequiredDatasetId();
            var sql = $"SELECT COUNT(1) FROM `{datasetId}`.INFORMATION_SCHEMA.TABLES WHERE table_type = 'BASE TABLE'";

            return (long)_rawSqlCommandBuilder.Build(sql).ExecuteScalar(
                new RelationalCommandParameterObject(_connection, null, null, null, null))! > 0;
        }

        private DbCommand CreateExistsCommand()
        {
            var datasetId = GetRequiredDatasetId();
            var projectId = GetRequiredProjectId();
            var sql = $"SELECT COUNT(1) FROM {Dependencies.SqlGenerationHelper.DelimitIdentifier(projectId)}.INFORMATION_SCHEMA.SCHEMATA WHERE schema_name = @datasetId";

            var command = _connection.DbConnection.CreateCommand();
            command.CommandText = sql;

            var parameter = command.CreateParameter();
            parameter.ParameterName = "@datasetId";
            parameter.Value = datasetId;
            command.Parameters.Add(parameter);

            return command;
        }

        /// <inheritdoc/>
        public override async Task<bool> HasTablesAsync(CancellationToken cancellationToken = default)
        {
            var datasetId = GetRequiredDatasetId();
            var sql = $"SELECT COUNT(1) FROM `{datasetId}`.INFORMATION_SCHEMA.TABLES WHERE table_type = 'BASE TABLE'";

            var result = await _rawSqlCommandBuilder.Build(sql).ExecuteScalarAsync(
                new RelationalCommandParameterObject(_connection, null, null, null, null),
                cancellationToken).ConfigureAwait(false);

            return (long)result! > 0;
        }

        private string GetRequiredDatasetId()
        {
            var datasetId = (_connection.DbConnection as BigQueryConnection)?.DefaultDatasetId;
            if (string.IsNullOrEmpty(datasetId))
            {
                throw new InvalidOperationException("A 'DefaultDatasetId' must be specified in the connection string to create or delete the database.");
            }
            return datasetId;
        }

        private string GetRequiredProjectId()
        {
            var projectId = (_connection.DbConnection as BigQueryConnection)?.DefaultProjectId;
            if (string.IsNullOrEmpty(projectId))
            {
                throw new InvalidOperationException("A 'DefaultProjectId' must be specified in the connection string to create or delete the database.");
            }
            return projectId;
        }

        private static bool IsDoesNotExist(GoogleApiException exception)
        {
            return exception.HttpStatusCode == System.Net.HttpStatusCode.NotFound;
        }
    }
}