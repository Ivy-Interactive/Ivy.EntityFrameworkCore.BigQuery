using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit.Abstractions;

namespace Ivy.EntityFrameworkCore.BigQuery.FunctionalTests.Query;
public class NorthwindJoinQueryBigQueryTest : NorthwindJoinQueryRelationalTestBase<NorthwindQueryBigQueryFixture<NoopModelCustomizer>>
{
    // ReSharper disable once UnusedParameter.Local
    public NorthwindJoinQueryBigQueryTest(NorthwindQueryBigQueryFixture<NoopModelCustomizer> fixture, ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
        ClearLog();
        Fixture.TestSqlLoggerFactory.SetTestOutputHelper(testOutputHelper);
    }

    protected override void ClearLog()
        => Fixture.TestSqlLoggerFactory.Clear();
}
