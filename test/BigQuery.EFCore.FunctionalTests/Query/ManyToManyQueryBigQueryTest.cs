using Microsoft.EntityFrameworkCore.Query;
using Xunit.Abstractions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query;

public class ManyToManyQueryBigQueryTest : ManyToManyQueryRelationalTestBase<ManyToManyQueryBigQueryFixture>
{
    public ManyToManyQueryBigQueryTest(ManyToManyQueryBigQueryFixture fixture, ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
        Fixture.TestSqlLoggerFactory.Clear();
        Fixture.TestSqlLoggerFactory.SetTestOutputHelper(testOutputHelper);
    }

    private void AssertSql(params string[] expected)
        => Fixture.TestSqlLoggerFactory.AssertBaseline(expected);

    #region Unsupported: Correlated subqueries with LIMIT/OFFSET

    [ConditionalTheory(Skip = "BigQuery does not support correlated subqueries with LIMIT/OFFSET")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Skip_navigation_order_by_single_or_default(bool async)
        => Task.CompletedTask;

    #endregion

    #region Unsupported: Deeply nested correlated subqueries

    [ConditionalTheory(Skip = "BigQuery does not support deeply nested correlated subqueries")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Filtered_include_skip_navigation_order_by_skip_take_then_include_skip_navigation_where(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support deeply nested correlated subqueries")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Filtered_include_skip_navigation_order_by_skip_take_then_include_skip_navigation_where_EF_Property(bool async)
        => Task.CompletedTask;

    #endregion
}
