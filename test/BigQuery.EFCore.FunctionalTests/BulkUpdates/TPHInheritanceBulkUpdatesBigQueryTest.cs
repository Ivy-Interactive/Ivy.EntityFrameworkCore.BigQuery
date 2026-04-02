using Microsoft.EntityFrameworkCore.BulkUpdates;
using Xunit.Abstractions;

namespace Ivy.EntityFrameworkCore.BigQuery.BulkUpdates;

public class TPHInheritanceBulkUpdatesBigQueryTest(
    TPHInheritanceBulkUpdatesBigQueryFixture fixture,
    ITestOutputHelper testOutputHelper)
    : TPHInheritanceBulkUpdatesTestBase<TPHInheritanceBulkUpdatesBigQueryFixture>(fixture, testOutputHelper)
{
    protected override void ClearLog()
        => Fixture.TestSqlLoggerFactory.Clear();

    private void AssertSql(params string[] expected)
        => Fixture.TestSqlLoggerFactory.AssertBaseline(expected);

    private void AssertExecuteUpdateSql(params string[] expected)
        => Fixture.TestSqlLoggerFactory.AssertBaseline(expected, forUpdate: true);

    #region Skipped: Correlated subqueries with LIMIT/OFFSET in DELETE/UPDATE

    [ConditionalTheory(Skip = "BigQuery does not support correlated subqueries with LIMIT/OFFSET in DELETE")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Delete_where_hierarchy_subquery(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support correlated subqueries with LIMIT/OFFSET in UPDATE")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Update_where_hierarchy_subquery(bool async)
        => Task.CompletedTask;

    #endregion

    #region Skipped: GroupBy with correlated subqueries

    [ConditionalTheory(Skip = "BigQuery does not support complex GroupBy with DELETE")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Delete_GroupBy_Where_Select_First(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support complex GroupBy with DELETE")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Delete_GroupBy_Where_Select_First_2(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support complex GroupBy with DELETE")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Delete_GroupBy_Where_Select_First_3(bool async)
        => Task.CompletedTask;

    #endregion
}