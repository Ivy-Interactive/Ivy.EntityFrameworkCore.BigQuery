using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit.Abstractions;
using Microsoft.EntityFrameworkCore.TestModels.Northwind;

namespace Ivy.EntityFrameworkCore.BigQuery.Query;

public class NorthwindAggregateOperatorsQueryBigQueryTest : NorthwindAggregateOperatorsQueryRelationalTestBase<
    NorthwindQueryBigQueryFixture<NoopModelCustomizer>>
{

    public NorthwindAggregateOperatorsQueryBigQueryTest(
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


    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public override async Task Cast_before_aggregate_is_preserved(bool async)
    {
        await AssertQueryScalar(
              async,
              ss => ss.Set<Customer>().Select(c => c.Orders.Select(o => (double?)o.OrderID).Average()),
              asserter: (e, a) =>
              {
                  Assert.Equal(e.HasValue, a.HasValue);
                  if (e.HasValue)
                      Assert.Equal(e.Value, a.Value, precision: 10);
              });

        AssertSql(
                """
                SELECT `s`.`_scalar_value`
                FROM `Customers` AS `c`
                LEFT JOIN (
                    SELECT AVG(CAST(`o`.`OrderID` AS FLOAT64)) AS `_scalar_value`, `o`.`CustomerID` AS `_partition0`
                    FROM `Orders` AS `o`
                    GROUP BY `o`.`CustomerID`
                ) AS `s` ON `c`.`CustomerID` = `s`.`_partition0`
                """);
    }

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public override Task Average_over_max_subquery(bool async)
      => AssertAverage(
          async,
          ss => ss.Set<Customer>().OrderBy(c => c.CustomerID).Take(3),
          selector: c => c.Orders.Average(o => 5 + o.OrderDetails.Max(od => od.ProductID))
          );


    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public override Task Average_over_nested_subquery(bool async)
      => AssertAverage(
          async,
          ss => ss.Set<Customer>().OrderBy(c => c.CustomerID).Take(3),
          selector: c => c.Orders.Average(o => 5 + o.OrderDetails.Average(od => od.ProductID)));

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public override Task Contains_inside_Average_without_GroupBy(bool async)
    {
        var cities = new[] { "London", "Berlin" };

        return AssertAverage(
            async,
            ss => ss.Set<Customer>(),
            selector: c => cities.Contains(c.City) ? 1.0 : 0.0,
            asserter: (e, a) => Assert.Equal(e, a, precision: 14));
    }

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public override async Task Type_casting_inside_sum(bool async)
    {
        await AssertSum(
              async,
              ss => ss.Set<OrderDetail>(),
              selector: x => (decimal)x.Discount,
              asserter: (e, a) => Assert.Equal((double)e, (double)a, precision: 10));

            AssertSql(
                """
                SELECT COALESCE(SUM(CAST(`o`.`Discount` AS BIGNUMERIC)), BIGNUMERIC '0')
                FROM `Order Details` AS `o`
                """);
    }

    #region Unsupported: Deeply nested correlated subqueries

    [ConditionalTheory(Skip = "BigQuery does not support deeply nested correlated subqueries")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Multiple_collection_navigation_with_FirstOrDefault_chained(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support deeply nested correlated subqueries")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Multiple_collection_navigation_with_FirstOrDefault_chained_projecting_scalar(bool async)
        => Task.CompletedTask;

    #endregion
}