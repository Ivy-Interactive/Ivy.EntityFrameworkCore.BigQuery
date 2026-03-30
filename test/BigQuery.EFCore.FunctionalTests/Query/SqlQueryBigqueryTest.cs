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
SELECT * FROM `Customers` WHERE `ContactName` LIKE '%z%'
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

    #region Parameter type inference limitations

    // EFCor expects parameters with same name/value but different Size to be treated as distinct.
    // BigQuery's parameter system doesn't use Size for parameter differentiation.
    [ConditionalTheory(Skip = "BigQuery does not differentiate parameters by Size metadata")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Multiple_occurrences_of_SqlQuery_with_db_parameter_adds_two_parameters(bool async)
        => base.Multiple_occurrences_of_SqlQuery_with_db_parameter_adds_two_parameters(async);

    // When uint? null is boxed and passed to SqlQueryRaw, the type information is lost.
    [ConditionalTheory(Skip = "BigQuery requires typed parameters - boxed null loses type information")]
    [MemberData(nameof(IsAsyncData))]
    public override Task SqlQueryRaw_queryable_with_null_parameter(bool async)
        => base.SqlQueryRaw_queryable_with_null_parameter(async);

    #endregion

    #region Exception type mismatch (InvalidCastException instead of InvalidOperationException)

    [ConditionalTheory(Skip = "BigQuery throws InvalidCastException instead of InvalidOperationException for DBNull")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Bad_data_error_handling_null(bool async)
        => base.Bad_data_error_handling_null(async);

    [ConditionalTheory(Skip = "BigQuery throws InvalidCastException instead of InvalidOperationException for DBNull")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Bad_data_error_handling_null_projection(bool async)
        => base.Bad_data_error_handling_null_projection(async);

    [ConditionalTheory(Skip = "BigQuery throws InvalidCastException instead of InvalidOperationException for DBNull")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Bad_data_error_handling_null_no_tracking(bool async)
        => base.Bad_data_error_handling_null_no_tracking(async);

    #endregion

    private void AssertSql(params string[] expected)
        => Fixture.TestSqlLoggerFactory.AssertBaseline(expected);
}