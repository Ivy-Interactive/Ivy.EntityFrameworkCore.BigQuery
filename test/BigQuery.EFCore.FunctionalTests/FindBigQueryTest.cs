using Ivy.EntityFrameworkCore.BigQuery.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Ivy.EntityFrameworkCore.BigQuery;

public abstract class FindBigQueryTest(FindBigQueryTest.FindBigQueryFixture fixture)
    : FindTestBase<FindBigQueryTest.FindBigQueryFixture>(fixture)
{
    // TODO: BigQuery returns owned collections as Dictionary<string,object>[] which cannot be cast to List<Owned1>
    [ConditionalFact(Skip = "BigQuery ADO.NET layer cannot materialize owned collections (Dictionary[] → List<Owned>)")]
    public override void Find_int_key_from_store()
    {
    }

    [ConditionalTheory(Skip = "BigQuery ADO.NET layer cannot materialize owned collections (Dictionary[] → List<Owned>)")]
    [InlineData(CancellationType.Right)]
    [InlineData(CancellationType.Wrong)]
    [InlineData(CancellationType.None)]
    public override Task Find_int_key_from_store_async(CancellationType cancellationType)
        => Task.CompletedTask;

    public class FindBigQueryTestSet(FindBigQueryFixture fixture) : FindBigQueryTest(fixture)
    {
        protected override TestFinder Finder { get; } = new FindViaSetFinder();
    }

    public class FindBigQueryTestContext(FindBigQueryFixture fixture) : FindBigQueryTest(fixture)
    {
        protected override TestFinder Finder { get; } = new FindViaContextFinder();
    }

    public class FindBigQueryTestNonGeneric(FindBigQueryFixture fixture) : FindBigQueryTest(fixture)
    {
        protected override TestFinder Finder { get; } = new FindViaNonGenericContextFinder();
    }

    public class FindBigQueryFixture : FindFixtureBase
    {
        public TestSqlLoggerFactory TestSqlLoggerFactory
            => (TestSqlLoggerFactory)ServiceProvider.GetRequiredService<ILoggerFactory>();

        protected override ITestStoreFactory TestStoreFactory
            => BigQueryTestStoreFactory.Instance;
    }
}