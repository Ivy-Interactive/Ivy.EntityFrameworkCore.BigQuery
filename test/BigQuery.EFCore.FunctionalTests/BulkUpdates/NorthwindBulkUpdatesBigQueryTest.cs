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
SET `c`.`ContactName` = 'Updated'
WHERE `c`.`CustomerID` LIKE 'F%'
""");
    }

    public override async Task Update_Where_set_constant_TagWith(bool async)
    {
        await base.Update_Where_set_constant_TagWith(async);

        AssertExecuteUpdateSql(
            """
-- MyUpdate

UPDATE `Customers` AS `c`
SET `c`.`ContactName` = 'Updated'
WHERE `c`.`CustomerID` LIKE 'F%'
""");
    }

    public override async Task Update_Where_parameter_set_constant(bool async)
    {
        await base.Update_Where_parameter_set_constant(async);

        AssertExecuteUpdateSql(
            """
@__customer_0='ALFKI'

UPDATE `Customers` AS `c`
SET `c`.`ContactName` = 'Updated'
WHERE `c`.`CustomerID` = @__customer_0
""",
            //
            """
@__customer_0='ALFKI'

UPDATE `Customers` AS `c`
SET `c`.`ContactName` = 'Updated'
WHERE `c`.`CustomerID` = @__customer_0
""");
    }

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
}