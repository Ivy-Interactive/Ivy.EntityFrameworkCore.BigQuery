using Ivy.Data.BigQuery;
using Ivy.EntityFrameworkCore.BigQuery.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using System.Data;
using System.Data.Common;

namespace Ivy.EntityFrameworkCore.BigQuery.Storage.Internal
{
    public class BigQueryRelationalConnection : RelationalConnection, IBigQueryRelationalConnection
    {

        protected override bool SupportsAmbientTransactions => false;

        public BigQueryRelationalConnection(RelationalConnectionDependencies dependencies)
            : base(dependencies)
        {
        }

        //public override IDbContextTransaction BeginTransaction() => new NoopTransaction();

        //public override Task<IDbContextTransaction> BeginTransactionAsync(CancellationToken cancellationToken = default)
        //=> Task.FromResult<IDbContextTransaction>(new NoopTransaction());

        public override void EnlistTransaction(System.Transactions.Transaction? transaction) {  }

        protected override DbConnection CreateDbConnection()
        {
            var connectionString = ConnectionString;
            return new BigQueryConnection(connectionString);
        }

        public override IDbContextTransaction? CurrentTransaction
        {
            get => null;
        }

        public virtual IBigQueryRelationalConnection CreateAdminConnection()
        {
            var adminConnectionStringBuilder = new BigQueryConnectionStringBuilder(ConnectionString)
            {
                DefaultDatasetId = null
            };
            var adminConnectionString = adminConnectionStringBuilder.ConnectionString;


            var bigQueryOptions = Dependencies.ContextOptions.FindExtension<BigQueryOptionsExtension>() ??
                throw new InvalidOperationException($"{nameof(BigQueryOptionsExtension)} not found in {nameof(CreateAdminConnection)}");

            var adminOptions = bigQueryOptions.WithConnectionString(adminConnectionString);
            var optionsBuilder = new DbContextOptionsBuilder();
            ((IDbContextOptionsBuilderInfrastructure)optionsBuilder).AddOrUpdateExtension(adminOptions);

            var adminDependencies = Dependencies with { ContextOptions = optionsBuilder.Options };

            return new BigQueryRelationalConnection(adminDependencies);
        }


        public override IDbContextTransaction BeginTransaction(IsolationLevel isolationLevel)
            => new NoopTransaction();

        public override Task<IDbContextTransaction> BeginTransactionAsync(IsolationLevel isolationLevel, CancellationToken cancellationToken = default)
            => Task.FromResult<IDbContextTransaction>(new NoopTransaction());

        private sealed class NoopTransaction : IDbContextTransaction
        {
            public Guid TransactionId => Guid.NewGuid();

            public void Commit() { }
            public Task CommitAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

            public void Rollback() {  }
            public Task RollbackAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

            public void Dispose() { }
            public ValueTask DisposeAsync() => ValueTask.CompletedTask;
        }
    }
}
