using Ivy.EntityFrameworkCore.BigQuery.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestModels.ManyToManyModel;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace Ivy.EntityFrameworkCore.BigQuery.Query;

public class ManyToManyQueryBigQueryFixture : ManyToManyQueryRelationalFixture
{
    protected override ITestStoreFactory TestStoreFactory
        => BigQueryTestStoreFactory.Instance;

    protected override void OnModelCreating(ModelBuilder modelBuilder, DbContext context)
    {
        base.OnModelCreating(modelBuilder, context);

        modelBuilder.Entity<EntityCompositeKey>().Property(e => e.Key3).HasColumnType("DATETIME");

        modelBuilder.Entity<JoinOneSelfPayload>().Property(e => e.Payload).HasColumnType("DATETIME");

        modelBuilder.Entity<JoinThreeToCompositeKeyFull>().Property(e => e.CompositeId3).HasColumnType("DATETIME");
        modelBuilder.Entity<JoinCompositeKeyToLeaf>().Property(e => e.CompositeId3).HasColumnType("DATETIME");

        modelBuilder.Entity<UnidirectionalEntityCompositeKey>().Property(e => e.Key3).HasColumnType("DATETIME");
        modelBuilder.Entity<UnidirectionalJoinThreeToCompositeKeyFull>().Property(e => e.CompositeId3).HasColumnType("DATETIME");
        modelBuilder.Entity<UnidirectionalJoinCompositeKeyToLeaf>().Property(e => e.CompositeId3).HasColumnType("DATETIME");
        modelBuilder.Entity<UnidirectionalJoinOneSelfPayload>().Property(e => e.Payload).HasColumnType("DATETIME");
    }
}
