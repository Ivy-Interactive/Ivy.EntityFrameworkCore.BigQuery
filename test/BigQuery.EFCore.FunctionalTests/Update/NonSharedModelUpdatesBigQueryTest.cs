using Ivy.EntityFrameworkCore.BigQuery.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.EntityFrameworkCore.Update;

namespace Ivy.EntityFrameworkCore.BigQuery.Update;

public class NonSharedModelUpdatesBigQueryTest : NonSharedModelUpdatesTestBase
{
    protected override ITestStoreFactory TestStoreFactory
        => BigQueryTestStoreFactory.Instance;

    [ConditionalTheory(Skip = "BigQuery doesn't support server-side key generation")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Principal_and_dependent_roundtrips_with_cycle_breaking(bool async)
        => base.Principal_and_dependent_roundtrips_with_cycle_breaking(async);

    [ConditionalTheory(Skip = "BigQuery doesn't support server-side key generation")]
    [MemberData(nameof(IsAsyncData))]
    public override Task DbUpdateException_Entries_is_correct_with_multiple_inserts(bool async)
        => base.DbUpdateException_Entries_is_correct_with_multiple_inserts(async);

    protected override Task ExecuteWithStrategyInTransactionAsync(
        ContextFactory<DbContext> contextFactory,
        Func<DbContext, Task> testOperation,
        Func<DbContext, Task>? nestedTestOperation1 = null,
        Func<DbContext, Task>? nestedTestOperation2 = null,
        Func<DbContext, Task>? nestedTestOperation3 = null)
        => TestHelpers.ExecuteWithStrategyInTransactionAsync(
            contextFactory.CreateContext,
            UseTransactionNoOp,
            testOperation,
            nestedTestOperation1,
            nestedTestOperation2,
            nestedTestOperation3);

    private static void UseTransactionNoOp(DatabaseFacade facade, IDbContextTransaction transaction)
    {
        // BigQuery doesn't support sharing transactions between contexts
    }
}