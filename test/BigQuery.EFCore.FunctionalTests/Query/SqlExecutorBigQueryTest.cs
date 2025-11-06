using Ivy.Data.BigQuery;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query;
using NetTopologySuite.Features;
using System.Data.Common;
using Xunit.Abstractions;

namespace Ivy.EntityFrameworkCore.BigQuery.FunctionalTests.Query;

public class SqlExecutorBigQueryTest : SqlExecutorTestBase<NorthwindQueryBigQueryFixture<SqlExecutorModelCustomizer>>
{
    public SqlExecutorBigQueryTest(NorthwindQueryBigQueryFixture<SqlExecutorModelCustomizer> fixture, ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
        Fixture.TestSqlLoggerFactory.Clear();
        Fixture.TestSqlLoggerFactory.SetTestOutputHelper(testOutputHelper);
    }

    private string GetDatasetName()
    {
        var builder = new BigQueryConnectionStringBuilder(Fixture.TestStore.ConnectionString);
        return builder.DefaultDatasetId;
    }

    protected override DbParameter CreateDbParameter(string name, object value)
      => new BigQueryParameter { ParameterName = name, Value = value };

    [ConditionalTheory]
    [InlineData(false)]
    [InlineData(true)]
    public override async Task Query_with_parameters(bool async)
    {
        var city = "London";
        var contactTitle = "Sales Representative";

        using var context = CreateContext();

        var actual = async
            ? await context.Database.ExecuteSqlRawAsync(
                $"SELECT COUNT(*) FROM `{Fixture.TestStore.Name}.Customers` WHERE `City` = {{0}} AND `ContactTitle` = {{1}}",
                city, contactTitle)
            : context.Database.ExecuteSqlRaw(
                $"SELECT COUNT(*) FROM `{Fixture.TestStore.Name}.Customers` WHERE `City` = {{0}} AND `ContactTitle` = {{1}}",
                city, contactTitle);

        Assert.Equal(-1, actual);
    }

    protected override string TenMostExpensiveProductsSproc
        => $"SELECT * FROM `{Fixture.TestStore.Name}.Ten_Most_Expensive_Products`()";

    protected override string CustomerOrderHistorySproc
        => $"SELECT * FROM `{Fixture.TestStore.Name}.CustOrderHist`(@CustomerID)";

    protected override string CustomerOrderHistoryWithGeneratedParameterSproc
        => $"SELECT * FROM `{Fixture.TestStore.Name}.CustOrderHist`({0})";
}