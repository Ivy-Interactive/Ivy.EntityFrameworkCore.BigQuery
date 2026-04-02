using Ivy.EntityFrameworkCore.BigQuery.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.BulkUpdates;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.TestModels.InheritanceModel;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace Ivy.EntityFrameworkCore.BigQuery.BulkUpdates;

public class TPTInheritanceBulkUpdatesBigQueryFixture : TPTInheritanceBulkUpdatesFixture
{
    // BQ doesn't support auto-generated keys
    public override bool UseGeneratedKeys => false;

    // Disable pooling to avoid service provider issues with relational facade dependencies
    protected override bool UsePooling => false;

    protected override ITestStoreFactory TestStoreFactory
        => BigQueryTestStoreFactory.Instance;

    // BQ doesn't support transactions
    public override void UseTransaction(DatabaseFacade facade, IDbContextTransaction transaction)
    {
        // BQ doesn't support transactions
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder, DbContext context)
    {
        base.OnModelCreating(modelBuilder, context);

        modelBuilder.Entity<Animal>().Property(e => e.Id).ValueGeneratedNever();
        modelBuilder.Entity<Drink>().Property(e => e.Id).ValueGeneratedNever();
    }
}
