using Ivy.Data.BigQuery;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;
using System.Data.Common;
using Xunit.Abstractions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query;

public class NorthwindSqlQueryBigQueryTest : NorthwindSqlQueryTestBase<NorthwindQueryBigQueryFixture<NoopModelCustomizer>>
{
    public NorthwindSqlQueryBigQueryTest(NorthwindQueryBigQueryFixture<NoopModelCustomizer> fixture, ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
        Fixture.TestSqlLoggerFactory.SetTestOutputHelper(testOutputHelper);
    }

    public override async Task SqlQueryRaw_over_int(bool async)
    {
        await base.SqlQueryRaw_over_int(async);

        AssertSql(
            """
SELECT `ProductID` FROM `Products`
""");
    }

    public override async Task SqlQuery_composed_Contains(bool async)
    {
        await base.SqlQuery_composed_Contains(async);

        AssertSql(
            """
SELECT `o`.`OrderID`, `o`.`CustomerID`, `o`.`EmployeeID`, `o`.`OrderDate`
FROM `Orders` AS `o`
WHERE `o`.`OrderID` IN (
    SELECT `s`.`Value`
    FROM (
        SELECT `ProductID` AS `Value` FROM `Products`
    ) AS `s`
)
""");
    }

    public override async Task SqlQuery_composed_Join(bool async)
    {
        await base.SqlQuery_composed_Join(async);

        AssertSql(
            """
SELECT `o`.`OrderID`, `o`.`CustomerID`, `o`.`EmployeeID`, `o`.`OrderDate`, CAST(`s`.`Value` AS INT64) AS `p`
FROM `Orders` AS `o`
INNER JOIN (
    SELECT `ProductID` AS `Value` FROM `Products`
) AS `s` ON `o`.`OrderID` = CAST(`s`.`Value` AS INT64)
""");
    }

    public override async Task SqlQuery_over_int_with_parameter(bool async)
    {
        await base.SqlQuery_over_int_with_parameter(async);

        AssertSql(
            """
@p0='10'

SELECT `ProductID` FROM `Products` WHERE `ProductID` = @p0
""");
    }

    [ConditionalFact]
    public virtual void Check_all_tests_overridden()
        => TestHelpers.AssertAllMethodsOverridden(GetType());

    protected override DbParameter CreateDbParameter(string name, object value)
        => new BigQueryParameter { ParameterName = name, Value = value };

    private void AssertSql(params string[] expected)
        => Fixture.TestSqlLoggerFactory.AssertBaseline(expected);
}
