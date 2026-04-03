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
    public override Task Delete_where_hierarchy_subquery(bool _)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support correlated subqueries with LIMIT/OFFSET in UPDATE")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Update_where_hierarchy_subquery(bool _)
        => Task.CompletedTask;

    #endregion

    #region Skipped: GroupBy with correlated subqueries

    [ConditionalTheory(Skip = "BigQuery does not support complex GroupBy with DELETE")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Delete_GroupBy_Where_Select_First(bool _)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support complex GroupBy with DELETE")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Delete_GroupBy_Where_Select_First_2(bool _)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support complex GroupBy with DELETE")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Delete_GroupBy_Where_Select_First_3(bool _)
        => Task.CompletedTask;

    #endregion

    #region Skipped: Correlated subqueries in DELETE WHERE clause

    [ConditionalTheory(Skip = "BigQuery does not support correlated subqueries in DELETE")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Delete_where_using_hierarchy(bool _)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support correlated subqueries in DELETE")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Delete_where_using_hierarchy_derived(bool _)
        => Task.CompletedTask;

    #endregion

    #region Skipped: Tests require transaction rollback (BigQuery does not support transactions)

    // These tests modify data (DELETE/UPDATE) and rely on transaction rollback to restore state.
    // BigQuery does not support transactions, so once data is deleted/updated, subsequent tests fail.

    [ConditionalTheory(Skip = "BigQuery does not support transactions - data cannot be restored after DELETE")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Delete_where_hierarchy(bool _)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support transactions - data cannot be restored after DELETE")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Delete_where_hierarchy_derived(bool _)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support transactions - data cannot be restored after UPDATE")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Update_base_type(bool _)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support transactions - data cannot be restored after UPDATE")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Update_base_type_with_OfType(bool _)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support transactions - data cannot be restored after UPDATE")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Update_base_property_on_derived_type(bool _)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support transactions - data cannot be restored after UPDATE")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Update_derived_property_on_derived_type(bool _)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support transactions - data cannot be restored after UPDATE")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Update_base_and_derived_types(bool _)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support transactions - data cannot be restored after UPDATE")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Update_where_using_hierarchy(bool _)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support transactions - data cannot be restored after UPDATE")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Update_where_using_hierarchy_derived(bool _)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support transactions - data cannot be restored after UPDATE")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Update_with_interface_in_property_expression(bool _)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support transactions - data cannot be restored after UPDATE")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Update_with_interface_in_EF_Property_in_property_expression(bool _)
        => Task.CompletedTask;

    #endregion
}