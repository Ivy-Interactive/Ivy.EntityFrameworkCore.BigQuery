using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit.Abstractions;

namespace Ivy.EntityFrameworkCore.BigQuery.FunctionalTests.Query;

public class NorthwindFunctionsQueryBigQueryTest : NorthwindFunctionsQueryRelationalTestBase<NorthwindQueryBigQueryFixture<NoopModelCustomizer>>
{
    public NorthwindFunctionsQueryBigQueryTest(
        NorthwindQueryBigQueryFixture<NoopModelCustomizer> fixture,
        ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
        ClearLog();
        Fixture.TestSqlLoggerFactory.SetTestOutputHelper(testOutputHelper);
    }
}
