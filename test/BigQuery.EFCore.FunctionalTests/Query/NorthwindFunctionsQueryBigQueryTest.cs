using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit.Abstractions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query;

public class NorthwindFunctionsQueryBigQueryTest : NorthwindFunctionsQueryRelationalTestBase<NorthwindQueryBigQueryFixture<NoopModelCustomizer>>
{
    public NorthwindFunctionsQueryBigQueryTest(
        NorthwindQueryBigQueryFixture<NoopModelCustomizer> fixture,
        ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
        ClearLog();
        Fixture.TestSqlLoggerFactory.SetTestOutputHelper(testOutputHelper);
    }

    #region Unsupported: BigQuery type conversion restrictions

    // BigQuery does not support certain type conversions that work in SQL Server
    // e.g., CAST(BIGNUMERIC AS BOOL) is not supported

    [ConditionalTheory(Skip = "BigQuery does not support casting BIGNUMERIC to BOOL")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Convert_ToBoolean(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support casting BOOL to BIGNUMERIC")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Convert_ToDecimal(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support casting BOOL to FLOAT64")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Convert_ToDouble(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support casting INTERVAL to FLOAT64")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Datetime_subtraction_TotalDays(bool async)
        => Task.CompletedTask;

    #endregion

}
