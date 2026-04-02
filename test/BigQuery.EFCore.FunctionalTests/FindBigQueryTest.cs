using Ivy.EntityFrameworkCore.BigQuery.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Ivy.EntityFrameworkCore.BigQuery;

public abstract class FindBigQueryTest(FindBigQueryTest.FindBigQueryFixture fixture)
    : FindTestBase<FindBigQueryTest.FindBigQueryFixture>(fixture)
{
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