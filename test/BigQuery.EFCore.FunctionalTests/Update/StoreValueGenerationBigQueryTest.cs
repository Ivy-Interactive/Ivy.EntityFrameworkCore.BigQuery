using Ivy.EntityFrameworkCore.BigQuery.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestModels.StoreValueGenerationModel;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.EntityFrameworkCore.Update;
using Xunit.Abstractions;

namespace Ivy.EntityFrameworkCore.BigQuery.Update;

public class StoreValueGenerationBigQueryTest : StoreValueGenerationTestBase<
    StoreValueGenerationBigQueryTest.StoreValueGenerationBigQueryFixture>
{
    public StoreValueGenerationBigQueryTest(StoreValueGenerationBigQueryFixture fixture, ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
        Fixture.TestSqlLoggerFactory.Clear();
        Fixture.TestSqlLoggerFactory.SetTestOutputHelper(testOutputHelper);
    }

    protected override bool ShouldCreateImplicitTransaction(
        EntityState firstOperationType,
        EntityState? secondOperationType,
        GeneratedValues generatedValues,
        bool withSameEntityType)
        => false;

    protected override int ShouldExecuteInNumberOfCommands(
        EntityState firstOperationType,
        EntityState? secondOperationType,
        GeneratedValues generatedValues,
        bool withDatabaseGenerated)
        => 1;

    private new void AssertSql(params string[] expected)
        => Fixture.TestSqlLoggerFactory.AssertBaseline(expected);

    #region Single operation

    [ConditionalTheory(Skip = "BigQuery does not support auto-increment integer IDs")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Add_with_generated_values(bool async)
        => Task.CompletedTask;

    public override async Task Add_with_no_generated_values(bool async)
    {
        await base.Add_with_no_generated_values(async);

        AssertSql(
            """
@p0='100'
@p1='1000'
@p2='1000'

INSERT INTO `WithNoDatabaseGenerated` (`Id`, `Data1`, `Data2`)
VALUES (@p0, @p1, @p2)
""");
    }

    public override async Task Modify_with_no_generated_values(bool async)
    {
        await base.Modify_with_no_generated_values(async);

        AssertSql(
            """
@p2='1'
@p0='1000'
@p1='1000'

UPDATE `WithNoDatabaseGenerated` SET `Data1` = @p0, `Data2` = @p1
WHERE `Id` = @p2
""");
    }

    public override async Task Delete(bool async)
    {
        await base.Delete(async);

        AssertSql(
            """
@p0='1'

DELETE FROM `WithSomeDatabaseGenerated`
WHERE `Id` = @p0
""");
    }

    #endregion Single operation

    #region Unsupported: Auto-generated IDs for WithSomeDatabaseGenerated

    [ConditionalTheory(Skip = "BigQuery does not support auto-increment integer IDs")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Add_Add_with_same_entity_type_and_generated_values(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support auto-increment integer IDs")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Add_Add_with_different_entity_types_and_generated_values(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support auto-increment integer IDs")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Delete_Add_with_same_entity_types(bool async)
        => Task.CompletedTask;

    #endregion

    #region Unsupported: Computed columns

    [ConditionalTheory(Skip = "BigQuery does not support computed columns")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Add_with_all_generated_values(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support computed columns")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Modify_with_generated_values(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support computed columns")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Add_Add_with_same_entity_type_and_all_generated_values(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support computed columns")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Add_Add_with_different_entity_types_and_all_generated_values(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support computed columns")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Modify_Modify_with_same_entity_type_and_generated_values(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support computed columns")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Modify_Modify_with_different_entity_types_and_generated_values(bool async)
        => Task.CompletedTask;

    #endregion

    public class StoreValueGenerationBigQueryFixture : StoreValueGenerationFixtureBase
    {
        protected override ITestStoreFactory TestStoreFactory
            => BigQueryTestStoreFactory.Instance;

        protected override void OnModelCreating(ModelBuilder modelBuilder, DbContext context)
        {
            // BigQuery doesn't support computed columns, so we use default values instead
            foreach (var name in new[]
                     {
                         nameof(StoreValueGenerationContext.WithNoDatabaseGenerated),
                         nameof(StoreValueGenerationContext.WithNoDatabaseGenerated2)
                     })
            {
                modelBuilder
                    .SharedTypeEntity<StoreValueGenerationData>(name)
                    .Property(w => w.Id)
                    .ValueGeneratedNever();
            }

            // For "some database generated" - use default values instead of computed columns
            // Set Id to ValueGeneratedNever since BigQuery doesn't support auto-increment
            foreach (var name in new[]
                     {
                         nameof(StoreValueGenerationContext.WithSomeDatabaseGenerated),
                         nameof(StoreValueGenerationContext.WithSomeDatabaseGenerated2)
                     })
            {
                modelBuilder
                    .SharedTypeEntity<StoreValueGenerationData>(name)
                    .Property(w => w.Id)
                    .ValueGeneratedNever();

                modelBuilder
                    .SharedTypeEntity<StoreValueGenerationData>(name)
                    .Property(w => w.Data1)
                    .HasDefaultValue(100)
                    .Metadata.ValueGenerated = Microsoft.EntityFrameworkCore.Metadata.ValueGenerated.OnAddOrUpdate;
            }

            // For "all database generated" - use default values
            foreach (var name in new[]
                     {
                         nameof(StoreValueGenerationContext.WithAllDatabaseGenerated),
                         nameof(StoreValueGenerationContext.WithAllDatabaseGenerated2)
                     })
            {
                modelBuilder
                    .SharedTypeEntity<StoreValueGenerationData>(name)
                    .Property(w => w.Data1)
                    .HasDefaultValue(80)
                    .Metadata.ValueGenerated = Microsoft.EntityFrameworkCore.Metadata.ValueGenerated.OnAddOrUpdate;

                modelBuilder
                    .SharedTypeEntity<StoreValueGenerationData>(name)
                    .Property(w => w.Data2)
                    .HasDefaultValue(81)
                    .Metadata.ValueGenerated = Microsoft.EntityFrameworkCore.Metadata.ValueGenerated.OnAddOrUpdate;
            }
        }

        protected override Task SeedAsync(StoreValueGenerationContext context)
        {
            // BigQuery doesn't support auto-increment IDs, so we can't seed WithAllDatabaseGenerated tables
            // (they would require INSERT INTO table () VALUES () which is invalid)

            context.WithSomeDatabaseGenerated.AddRange(
                new StoreValueGenerationData { Id = 1, Data2 = 1 },
                new StoreValueGenerationData { Id = 2, Data2 = 2 });
            context.WithSomeDatabaseGenerated2.AddRange(
                new StoreValueGenerationData { Id = 1, Data2 = 1 },
                new StoreValueGenerationData { Id = 2, Data2 = 2 });

            context.WithNoDatabaseGenerated.AddRange(
                new StoreValueGenerationData { Id = 1, Data1 = 10, Data2 = 20 },
                new StoreValueGenerationData { Id = 2, Data1 = 11, Data2 = 21 });
            context.WithNoDatabaseGenerated2.AddRange(
                new StoreValueGenerationData { Id = 1, Data1 = 10, Data2 = 20 },
                new StoreValueGenerationData { Id = 2, Data1 = 11, Data2 = 21 });

            // Skip WithAllDatabaseGenerated - BigQuery can't handle tables with all generated values

            return context.SaveChangesAsync();
        }

        public override void CleanData()
        {
            using var context = CreateContext();

            context.WithSomeDatabaseGenerated.RemoveRange(context.WithSomeDatabaseGenerated);
            context.WithSomeDatabaseGenerated2.RemoveRange(context.WithSomeDatabaseGenerated2);

            context.WithNoDatabaseGenerated.RemoveRange(context.WithNoDatabaseGenerated);
            context.WithNoDatabaseGenerated2.RemoveRange(context.WithNoDatabaseGenerated2);

            // Skip WithAllDatabaseGenerated - these tables aren't seeded

            context.SaveChanges();
        }
    }
}