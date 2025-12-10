using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestModels.GearsOfWarModel;
using Xunit.Abstractions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query;

public class TPCGearsOfWarQueryBigQueryTest : TPCGearsOfWarQueryRelationalTestBase<TPCGearsOfWarQueryBigQueryFixture>
{
    public TPCGearsOfWarQueryBigQueryTest(TPCGearsOfWarQueryBigQueryFixture fixture, ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
        Fixture.TestSqlLoggerFactory.Clear();
        Fixture.TestSqlLoggerFactory.SetTestOutputHelper(testOutputHelper);
    }    

    public override async Task Select_datetimeoffset_comparison_in_projection(bool async)
    {
        await AssertQueryScalar(
            async,
            ss => ss.Set<Mission>().Select(m => m.Timeline > DateTimeOffset.UtcNow));

        AssertSql(
            """
SELECT m.`Timeline` > CURRENT_DATETIME()
FROM `Missions` AS m
""");
    }

    private void AssertSql(params string[] expected)
        => Fixture.TestSqlLoggerFactory.AssertBaseline(expected);
}