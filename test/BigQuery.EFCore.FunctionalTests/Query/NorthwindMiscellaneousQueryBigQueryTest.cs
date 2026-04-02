using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit.Abstractions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query;

public class NorthwindMiscellaneousQueryBigQueryTest : NorthwindMiscellaneousQueryRelationalTestBase<NorthwindQueryBigQueryFixture<NoopModelCustomizer>>
{
    // ReSharper disable once UnusedParameter.Local
    public NorthwindMiscellaneousQueryBigQueryTest(
        NorthwindQueryBigQueryFixture<NoopModelCustomizer> fixture,
        ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
        ClearLog();
        Fixture.TestSqlLoggerFactory.SetTestOutputHelper(testOutputHelper);
    }

    private void AssertSql(params string[] expected)
        => Fixture.TestSqlLoggerFactory.AssertBaseline(expected);

    protected override void ClearLog()
        => Fixture.TestSqlLoggerFactory.Clear();

    #region Skipped: TimeSpan not fully supported

    [ConditionalTheory(Skip = "BigQuery does not have native TimeSpan/INTERVAL arithmetic like SQL Server")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Add_minutes_on_constant_value(bool async)
        => Task.CompletedTask;

    #endregion

    #region Skipped: Correlated subqueries with specific patterns

    [ConditionalTheory(Skip = "BigQuery does not support correlated subqueries with LIMIT/OFFSET in this context")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Max_on_empty_sequence_throws(bool async)
        => Task.CompletedTask;

    #endregion
}
