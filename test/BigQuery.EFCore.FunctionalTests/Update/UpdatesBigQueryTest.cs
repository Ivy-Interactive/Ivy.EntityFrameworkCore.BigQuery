using Ivy.EntityFrameworkCore.BigQuery.Extensions;
using Ivy.EntityFrameworkCore.BigQuery.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.TestModels.UpdatesModel;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.EntityFrameworkCore.Update;
using Xunit.Abstractions;

namespace Ivy.EntityFrameworkCore.BigQuery.Update;

public class UpdatesBigQueryTest : UpdatesRelationalTestBase<UpdatesBigQueryTest.UpdatesBigQueryFixture>
{
    public UpdatesBigQueryTest(UpdatesBigQueryFixture fixture, ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
        Fixture.TestSqlLoggerFactory.Clear();
    }

    // BigQuery has a 128 character limit for identifiers
    public override void Identifiers_are_generated_correctly()
    {
        using var context = CreateContext();

        var entityType = context.Model.FindEntityType(
            typeof(
                LoginEntityTypeWithAnExtremelyLongAndOverlyConvolutedNameThatIsUsedToVerifyThatTheStoreIdentifierGenerationLengthLimitIsWorkingCorrectly
            ));
        Assert.Equal(
            "LoginEntityTypeWithAnExtremelyLongAndOverlyConvolutedNameThatIsUsedToVerifyThatTheStoreIdentifierGenerationLengthLimitIsWorki",
            entityType!.GetTableName());
        Assert.Equal(
            "PK_LoginEntityTypeWithAnExtremelyLongAndOverlyConvolutedNameThatIsUsedToVerifyThatTheStoreIdentifierGenerationLengthLimitIsWo",
            entityType.GetKeys().Single().GetName());
        Assert.Equal(
            "FK_LoginEntityTypeWithAnExtremelyLongAndOverlyConvolutedNameThatIsUsedToVerifyThatTheStoreIdentifierGenerationLengthLimitIsWo",
            entityType.GetForeignKeys().Single().GetConstraintName());
        Assert.Equal(
            "IX_LoginEntityTypeWithAnExtremelyLongAndOverlyConvolutedNameThatIsUsedToVerifyThatTheStoreIdentifierGenerationLengthLimitIsWo",
            entityType.GetIndexes().Single().GetDatabaseName());

        var entityType2 = context.Model.FindEntityType(
            typeof(
                LoginEntityTypeWithAnExtremelyLongAndOverlyConvolutedNameThatIsUsedToVerifyThatTheStoreIdentifierGenerationLengthLimitIsWorkingCorrectlyDetails
            ));

        Assert.Equal(
            "LoginEntityTypeWithAnExtremelyLongAndOverlyConvolutedNameThatIsUsedToVerifyThatTheStoreIdentifierGenerationLengthLimitIsWor1",
            entityType2!.GetTableName());
        Assert.Equal(
            "PK_LoginDetails",
            entityType2.GetKeys().Single().GetName());
    }

    protected override string UpdateConcurrencyMessage
        => "BigQuery does not support optimistic concurrency";

    protected override string UpdateConcurrencyTokenMessage
        => "BigQuery does not support optimistic concurrency";

    protected override Task ExecuteWithStrategyInTransactionAsync(
        Func<UpdatesContext, Task> testOperation,
        Func<UpdatesContext, Task>? nestedTestOperation1 = null,
        Func<UpdatesContext, Task>? nestedTestOperation2 = null)
    {
        // BigQuery doesn't support transactions, so just execute the test operations directly
        return TestHelpers.ExecuteWithStrategyInTransactionAsync(
            CreateContext,
            UseTransaction,
            testOperation,
            nestedTestOperation1,
            nestedTestOperation2);
    }

    protected override void UseTransaction(DatabaseFacade facade, IDbContextTransaction transaction)
    {
        // BigQuery doesn't support sharing transactions between contexts
    }

    #region Unsupported: Concurrency tokens

    [ConditionalTheory(Skip = "BigQuery does not support optimistic concurrency")]
    [InlineData(false)]
    [InlineData(true)]
    public override Task Can_change_type_of_pk_to_pk_dependent_by_replacing_with_new_dependent(bool async)
        => Task.CompletedTask;

    #endregion

    // #region Unsupported: Views
    //
    // [ConditionalFact(Skip = "BigQuery does not support views in this context")]
    // public override Task SaveChanges_works_for_entities_also_mapped_to_view()
    //     => Task.CompletedTask;
    //
    // [ConditionalFact(Skip = "BigQuery does not support views in this context")]
    // public override Task SaveChanges_throws_for_entities_only_mapped_to_view()
    //     => Task.CompletedTask;
    //
    // #endregion

    #region Unsupported: Auto-generated integer IDs

    [ConditionalFact(Skip = "BigQuery does not support auto-increment integer IDs")]
    public override Task Save_with_shared_foreign_key()
        => Task.CompletedTask;

    [ConditionalFact(Skip = "BigQuery does not support auto-increment integer IDs")]
    public override Task Can_use_shared_columns_with_conversion()
        => Task.CompletedTask;

    #endregion

    public class UpdatesBigQueryFixture : UpdatesRelationalFixture
    {
        // Use custom test store that ignores unique constraints
        protected override ITestStoreFactory TestStoreFactory
            => UpdatesBigQueryTestStoreFactory.Instance;

        protected override void OnModelCreating(ModelBuilder modelBuilder, DbContext context)
        {
            base.OnModelCreating(modelBuilder, context);

            // BigQuery uses GENERATE_UUID() for UUID generation
            modelBuilder.Entity<ProductBase>()
                .Property(p => p.Id).HasDefaultValueSql("GENERATE_UUID()");

            // BigQuery uses DATETIME for concurrency tokens
            modelBuilder.Entity<Rodney>().Property(r => r.Concurrency).HasColumnType("DATETIME");

            // BigQuery doesn't support auto-increment integer IDs - use explicit IDs
            modelBuilder.Entity<Category>().Property(c => c.Id).ValueGeneratedNever();
        }

        protected override Task SeedAsync(UpdatesContext context)
        {
            var productId1 = new Guid("984ade3c-2f7b-4651-a351-642e92ab7146");
            var productId2 = new Guid("0edc9136-7eed-463b-9b97-bdb9648ab877");

            // Explicitly set Category.Id since BigQuery doesn't support auto-increment
            context.Add(new Category { Id = 1, PrincipalId = 778 });
            context.Add(
                new Product
                {
                    Id = productId1,
                    Name = "Apple Cider",
                    Price = 1.49M,
                    DependentId = 778,
                    IsPrimary = true
                });
            context.Add(
                new Product
                {
                    Id = productId2,
                    Name = "Apple Cobler",
                    Price = 2.49M,
                    DependentId = 778,
                    IsPrimary = false
                });

            return context.SaveChangesAsync();
        }
    }
}

/// <summary>
/// Custom test store factory that enables IgnoreUniqueConstraints for Updates tests.
/// </summary>
public class UpdatesBigQueryTestStoreFactory : BigQueryTestStoreFactory
{
    public new static UpdatesBigQueryTestStoreFactory Instance { get; } = new();

    public override TestStore Create(string storeName)
        => new UpdatesBigQueryTestStore(storeName, shared: false);

    public override TestStore GetOrCreate(string storeName)
        => new UpdatesBigQueryTestStore(storeName, shared: true);
}

public class UpdatesBigQueryTestStore : BigQueryTestStore
{
    public UpdatesBigQueryTestStore(string name, bool shared) : base(name, shared)
    {
    }

    public override DbContextOptionsBuilder AddProviderOptions(DbContextOptionsBuilder builder)
    {
        return builder.UseBigQuery(Connection.ConnectionString, b =>
        {
            b.IgnoreUniqueConstraints();
            b.ApplyConfiguration();
        });
    }
}