using Ivy.Data.BigQuery;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestModels.Northwind;
using Microsoft.EntityFrameworkCore.TestUtilities;
using System.Data.Common;
using Xunit.Abstractions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query;

public class SqlQueryBigQueryTest : SqlQueryTestBase<NorthwindQueryBigQueryFixture<NoopModelCustomizer>>
{
    public SqlQueryBigQueryTest(NorthwindQueryBigQueryFixture<NoopModelCustomizer> fixture, ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
        Fixture.TestSqlLoggerFactory.SetTestOutputHelper(testOutputHelper);
    }

    public override async Task SqlQueryRaw_queryable_simple(bool async)
    {
        await base.SqlQueryRaw_queryable_simple(async);

        AssertSql(
            """
SELECT
    *
  FROM
    Customers
  WHERE Customers.ContactName LIKE '%z%'
;
""");
    }


    public override async Task SqlQueryRaw_with_dbParameter_without_name_prefix(bool async)
    {
        var parameter = CreateDbParameter("city", "London");

        await AssertQuery(
            async,
            _ => Fixture.CreateContext().Database.SqlQueryRaw<UnmappedCustomer>(
                NormalizeDelimitersInRawString("SELECT * FROM [Customers] WHERE [City] = @city"), parameter),
            ss => ss.Set<Customer>().Where(x => x.City == "London").Select(e => UnmappedCustomer.FromCustomer(e)),
            elementSorter: e => e.CustomerID,
            elementAsserter: AssertUnmappedCustomers);
    }

      private static void AssertUnmappedCustomers(UnmappedCustomer l, UnmappedCustomer r)
    {
        Assert.Equal(l.CustomerID, r.CustomerID);
        Assert.Equal(l.CompanyName, r.CompanyName);
        Assert.Equal(l.ContactName, r.ContactName);
        Assert.Equal(l.ContactTitle, r.ContactTitle);
        Assert.Equal(l.City, r.City);
        Assert.Equal(l.Region, r.Region);
        Assert.Equal(l.Zip, r.Zip);
        Assert.Equal(l.Country, r.Country);
        Assert.Equal(l.Phone, r.Phone);
        Assert.Equal(l.Fax, r.Fax);
    }

 

    protected override DbParameter CreateDbParameter(string name, object value)
    => new BigQueryParameter { ParameterName = name, Value = value };

    private void AssertSql(params string[] expected)
        => Fixture.TestSqlLoggerFactory.AssertBaseline(expected);
}