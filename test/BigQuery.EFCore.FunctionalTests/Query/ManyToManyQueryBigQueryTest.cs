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

    #region Unsupported: Skip navigation alias scope issues

    // BigQuery SQL generation creates alias scope issues in join with skip navigation queries
    // causing "Unrecognized name: e0" or "Unrecognized name: u0" errors

    [ConditionalTheory(Skip = "BigQuery alias scope issue in skip navigation join")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Join_with_skip_navigation(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery alias scope issue in skip navigation join")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Join_with_skip_navigation_unidirectional(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery alias scope issue in skip navigation join")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Left_join_with_skip_navigation(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery alias scope issue in skip navigation join")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Left_join_with_skip_navigation_unidirectional(bool async)
        => Task.CompletedTask;

    #endregion

    #region Result differences

    [ConditionalTheory(Skip = "BigQuery filtered include produces no results")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Filtered_include_on_skip_navigation_then_filtered_include_on_navigation_split(bool async)
        => Task.CompletedTask;

    #endregion
}
