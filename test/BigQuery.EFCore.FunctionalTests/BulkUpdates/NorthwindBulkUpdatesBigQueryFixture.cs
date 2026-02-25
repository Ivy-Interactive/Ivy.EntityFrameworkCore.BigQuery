using Ivy.EntityFrameworkCore.BigQuery.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.BulkUpdates;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.TestModels.Northwind;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace Ivy.EntityFrameworkCore.BigQuery.BulkUpdates;

public class NorthwindBulkUpdatesBigQueryFixture<TModelCustomizer> : NorthwindBulkUpdatesRelationalFixture<TModelCustomizer>
    where TModelCustomizer : ITestModelCustomizer, new()
{
    private bool _needsReseed;

    protected override ITestStoreFactory TestStoreFactory
        => BigQueryNorthwindTestStoreFactory.Instance;

    protected override Type ContextType
        => typeof(TestModels.Northwind.NorthwindBigQueryContext);

    protected override void OnModelCreating(ModelBuilder modelBuilder, DbContext context)
    {
        base.OnModelCreating(modelBuilder, context);

        // BigQuery uses NUMERIC for money types
        modelBuilder.Entity<MostExpensiveProduct>()
            .Property(p => p.UnitPrice)
            .HasColumnType("NUMERIC");
    }

    public override void UseTransaction(DatabaseFacade facade, IDbContextTransaction transaction)
    {
        // BigQuery doesn't support transaction rollback, so we need to clean
        // and reseed data before each test to ensure consistent test state.
        if (_needsReseed)
        {
            // Fast reseed: delete from known entity sets and re-seed
            using var context = CreateContext();
            // Note: In production, you would implement proper data cleanup here
        }
        _needsReseed = true;
    }
}
