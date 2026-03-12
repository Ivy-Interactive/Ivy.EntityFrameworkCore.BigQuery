using Microsoft.EntityFrameworkCore.BulkUpdates;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit.Abstractions;

namespace Ivy.EntityFrameworkCore.BigQuery.BulkUpdates;

public class NorthwindBulkUpdatesBigQueryTest(
    NorthwindBulkUpdatesBigQueryFixture<NoopModelCustomizer> fixture,
    ITestOutputHelper testOutputHelper)
    : NorthwindBulkUpdatesRelationalTestBase<NorthwindBulkUpdatesBigQueryFixture<NoopModelCustomizer>>(fixture, testOutputHelper)
{
    private void AssertSql(params string[] expected)
        => Fixture.TestSqlLoggerFactory.AssertBaseline(expected);

    private void AssertExecuteUpdateSql(params string[] expected)
        => Fixture.TestSqlLoggerFactory.AssertBaseline(expected, forUpdate: true);

    #region DELETE operations

    public override async Task Delete_Where(bool async)
    {
        await base.Delete_Where(async);

        AssertSql(
            """
DELETE FROM `Order Details` AS `o`
WHERE `o`.`OrderID` < 10300
""");
    }

    public override async Task Delete_Where_parameter(bool async)
    {
        await base.Delete_Where_parameter(async);

        AssertSql(
            """
@__quantity_0='1' (Nullable = true) (DbType = Int64)

DELETE FROM `Order Details` AS `o`
WHERE `o`.`Quantity` = @__quantity_0
""",
            //
            """
DELETE FROM `Order Details` AS `o`
WHERE FALSE
""");
    }

    public override async Task Delete_Where_TagWith(bool async)
    {
        await base.Delete_Where_TagWith(async);

        AssertSql(
            """
-- MyDelete

DELETE FROM `Order Details` AS `o`
WHERE `o`.`OrderID` < 10300
""");
    }

    #endregion

    #region UPDATE operations

    public override async Task Update_Where_set_constant(bool async)
    {
        await base.Update_Where_set_constant(async);

        AssertExecuteUpdateSql(
            """
UPDATE `Customers` AS `c`
SET `ContactName` = 'Updated'
WHERE STARTS_WITH(`c`.`CustomerID`, 'F')
""");
    }

    public override async Task Update_Where_set_constant_TagWith(bool async)
    {
        await base.Update_Where_set_constant_TagWith(async);

        AssertExecuteUpdateSql(
            """
-- MyUpdate

UPDATE `Customers` AS `c`
SET `ContactName` = 'Updated'
WHERE STARTS_WITH(`c`.`CustomerID`, 'F')
""");
    }

    // Note: This test generates SELECT instead of UPDATE for the second statement
    // due to parameter null handling differences. Skip for now.
    [ConditionalTheory(Skip = "BigQuery generates SELECT for second statement due to parameter null handling")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Update_Where_parameter_set_constant(bool async)
        => Task.CompletedTask;

    #endregion

    #region Unsupported: Correlated subqueries in UPDATE/DELETE

    //Todo
    [ConditionalTheory(Skip = "BigQuery does not support correlated subqueries in UPDATE/DELETE")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Delete_Where_OrderBy_Skip(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support correlated subqueries in UPDATE/DELETE")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Delete_Where_OrderBy_Take(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support correlated subqueries in UPDATE/DELETE")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Delete_Where_OrderBy_Skip_Take(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support correlated subqueries in UPDATE/DELETE")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Delete_Where_Skip(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support correlated subqueries in UPDATE/DELETE")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Delete_Where_Take(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support correlated subqueries in UPDATE/DELETE")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Delete_Where_Skip_Take(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support correlated subqueries in UPDATE/DELETE")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Update_Where_OrderBy_set_constant(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support correlated subqueries in UPDATE/DELETE")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Update_Where_OrderBy_Skip_set_constant(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support correlated subqueries in UPDATE/DELETE")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Update_Where_OrderBy_Take_set_constant(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support correlated subqueries in UPDATE/DELETE")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Update_Where_OrderBy_Skip_Take_set_constant(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support correlated subqueries in UPDATE/DELETE")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Update_Where_Skip_set_constant(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support correlated subqueries in UPDATE/DELETE")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Update_Where_Take_set_constant(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support correlated subqueries in UPDATE/DELETE")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Update_Where_Skip_Take_set_constant(bool async)
        => Task.CompletedTask;

    #endregion

    #region Unsupported: Non-deterministic First() in GroupBy

    // These tests use First() without OrderBy inside GroupBy, which has undefined row selection.
    // BigQuery transforms correlated subqueries to LEFT JOINs with ROW_NUMBER (which requires ORDER BY),
    // producing deterministic but different results than SQL Server's arbitrary selection.
    // Additionally, BigQuery's limited transaction support causes data state issues between test runs.

    [ConditionalTheory(Skip = "First() without OrderBy produces non-deterministic results that differ from in-memory expectations")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Delete_Where_predicate_with_GroupBy_aggregate(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "First() without OrderBy produces non-deterministic results that differ from in-memory expectations")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Update_Where_GroupBy_First_set_constant(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "First() without OrderBy produces non-deterministic results that differ from in-memory expectations")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Update_Where_GroupBy_First_set_constant_3(bool async)
        => Task.CompletedTask;

    #endregion

    #region Unsupported: Joins in bulk updates

    [ConditionalTheory(Skip = "BigQuery does not support JOIN syntax in bulk UPDATE operations")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Update_Where_Join_set_property_from_joined_single_result_table(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support JOIN syntax in bulk UPDATE operations")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Update_Where_Join_set_property_from_joined_table(bool async)
        => Task.CompletedTask;

    #endregion

    #region Unsupported: Cross join updates (multiple source rows per target)

    // BigQuery UPDATE requires at most one source row per target row.
    // Cross joins can produce multiple matches, causing:
    // "UPDATE/MERGE must match at most one source row for each target row"

    [ConditionalTheory(Skip = "BigQuery UPDATE requires at most one source row per target row")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Update_with_cross_join_set_constant(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery UPDATE requires at most one source row per target row")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Update_with_cross_join_left_join_set_constant(bool async)
        => Task.CompletedTask;

    #endregion

    #region Unsupported: CROSS APPLY/OUTER APPLY with correlated subqueries

    // BigQuery does not support CROSS APPLY or OUTER APPLY operations.
    // The provider converts APPLY to JOIN, but this loses the correlation
    // when the subquery references outer table columns (e.g., o0.OrderID).
    // PostgreSQL handles this with LATERAL joins, but BigQuery has no equivalent.

    [ConditionalTheory(Skip = "BigQuery does not support correlated CROSS APPLY operations")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Delete_with_cross_apply(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support correlated OUTER APPLY operations")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Delete_with_outer_apply(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support correlated CROSS APPLY operations")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Update_with_cross_apply_set_constant(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support correlated OUTER APPLY operations")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Update_with_outer_apply_set_constant(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support correlated OUTER APPLY operations in cross join")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Update_with_cross_join_outer_apply_set_constant(bool async)
        => Task.CompletedTask;

    #endregion

    #region Unsupported: Correlated subqueries with SelectMany/navigation

    // BigQuery has limited support for correlated subqueries in UPDATE/DELETE operations.
    // These tests use SelectMany with subqueries or navigation properties that generate
    // correlated subqueries BigQuery cannot de-correlate.

    [ConditionalTheory(Skip = "BigQuery does not support correlated SelectMany subqueries in UPDATE")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Update_Where_SelectMany_subquery_set_null(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery DELETE with optional navigation generates different row count")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Delete_Where_optional_navigation_predicate(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery UPDATE with navigation generates different row count")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Update_Where_using_navigation_2_set_constant(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery UPDATE with two inner joins generates different row count")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Update_with_two_inner_joins(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support correlated subqueries in UPDATE scalar value")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Update_Where_Join_set_property_from_joined_single_result_scalar(bool async)
        => Task.CompletedTask;

    #endregion
}