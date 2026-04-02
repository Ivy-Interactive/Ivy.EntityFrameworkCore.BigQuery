using Ivy.EntityFrameworkCore.BigQuery.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestModels.InheritanceModel;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace Ivy.EntityFrameworkCore.BigQuery.Query;

public class TPHInheritanceQueryBigQueryFixture : TPHInheritanceQueryFixture
{
    // BigQuery doesn't support auto-generated keys
    public override bool UseGeneratedKeys => false;

    protected override ITestStoreFactory TestStoreFactory
        => BigQueryTestStoreFactory.Instance;

    protected override void OnModelCreating(ModelBuilder modelBuilder, DbContext context)
    {
        base.OnModelCreating(modelBuilder, context);

        // BigQuery doesn't support auto-increment, use explicit IDs
        modelBuilder.Entity<Animal>().Property(e => e.Id).ValueGeneratedNever();
        modelBuilder.Entity<Drink>().Property(e => e.Id).ValueGeneratedNever();

        modelBuilder.Entity<AnimalQuery>().HasNoKey().ToSqlQuery("""
            SELECT * FROM `Animals`
            """);
    }
}