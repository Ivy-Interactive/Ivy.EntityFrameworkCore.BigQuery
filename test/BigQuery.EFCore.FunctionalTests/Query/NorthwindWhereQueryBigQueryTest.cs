using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit.Abstractions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query;

public class NorthwindWhereQueryBigQueryTest : NorthwindWhereQueryRelationalTestBase<NorthwindQueryBigQueryFixture<NoopModelCustomizer>>
{
    public NorthwindWhereQueryBigQueryTest(NorthwindQueryBigQueryFixture<NoopModelCustomizer> fixture, ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
        Fixture.TestSqlLoggerFactory.Clear();
        Fixture.TestSqlLoggerFactory.SetTestOutputHelper(testOutputHelper);
    }

    public override async Task Where_ternary_boolean_condition_negated(bool async)
    {
        await base.Where_ternary_boolean_condition_negated(async);

        AssertSql(
            """
SELECT `p`.`ProductID`, `p`.`Discontinued`, `p`.`ProductName`, `p`.`SupplierID`, `p`.`UnitPrice`, `p`.`UnitsInStock`
FROM `Products` AS `p`
WHERE CASE
    WHEN `p`.`UnitsInStock` >= 20 THEN 1
    ELSE 0
END
""");
    }

    private void AssertSql(params string[] expected)
        => Fixture.TestSqlLoggerFactory.AssertBaseline(expected);
}
