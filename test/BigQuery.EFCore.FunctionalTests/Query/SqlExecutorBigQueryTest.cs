using Ivy.Data.BigQuery;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;
using System.Data.Common;
using System.Runtime.CompilerServices;
using Xunit.Abstractions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query;

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

    [ConditionalTheory]
    [InlineData(false)]
    [InlineData(true)]
    public override async Task Throws_on_concurrent_command(bool async)
    {
        using var context = CreateContext();
        context.Database.EnsureCreatedResiliently();

        using var synchronizationEvent = new ManualResetEventSlim(false);
        using var blockingSemaphore = new SemaphoreSlim(0);
        var blockingTask = Task.Run(
            () =>
                context.Customers.Select(
                    c => Process(c, synchronizationEvent, blockingSemaphore)).ToList());

        if (async)
        {
            var throwingTask = Task.Run(
                async () =>
                {
                    synchronizationEvent.Wait();
                    Assert.Equal(
                        CoreStrings.ConcurrentMethodInvocation,
                        (await Assert.ThrowsAsync<InvalidOperationException>(
                            () => context.Database.ExecuteSqlAsync($@"SELECT * FROM `{Fixture.TestStore.Name}`.`Customers`"))).Message);
                });

            await throwingTask;
        }
        else
        {
            var throwingTask = Task.Run(
                () =>
                {
                    synchronizationEvent.Wait();
                    Assert.Equal(
                        CoreStrings.ConcurrentMethodInvocation,
                        Assert.Throws<InvalidOperationException>(
                            () => context.Database.ExecuteSql($@"SELECT * FROM `{Fixture.TestStore.Name}`.`Customers`")).Message);
                });

            throwingTask.Wait();
        }

        blockingSemaphore.Release(1);

        blockingTask.Wait();
    }

    protected override string TenMostExpensiveProductsSproc
        => $"SELECT * FROM `{Fixture.TestStore.Name}.Ten_Most_Expensive_Products`()";

    protected override string CustomerOrderHistorySproc
        => $"SELECT * FROM `{Fixture.TestStore.Name}.CustOrderHist`(@CustomerID)";

    protected override string CustomerOrderHistoryWithGeneratedParameterSproc
        => $"SELECT * FROM `{Fixture.TestStore.Name}.CustOrderHist`({0})";

    [ConditionalTheory]
    [InlineData(false)]
    [InlineData(true)]
    public override async Task Query_with_dbParameter_with_name(bool async)
    {
        var city = CreateDbParameter("@city", "London");

        using var context = CreateContext();

        var actual = async
            ? await context.Database.ExecuteSqlRawAsync($@"SELECT COUNT(*) FROM `{Fixture.TestStore.Name}.Customers` WHERE `City` = @city", city)
            : context.Database.ExecuteSqlRaw($@"SELECT COUNT(*) FROM `{Fixture.TestStore.Name}.Customers` WHERE `City` = @city", city);

        Assert.Equal(-1, actual);
    }

    [ConditionalTheory]
    [InlineData(false)]
    [InlineData(true)]
    public override async Task Query_with_positional_dbParameter_with_name(bool async)
    {
        var city = CreateDbParameter("@city", "London");

        using var context = CreateContext();

        var actual = async
            ? await context.Database.ExecuteSqlRawAsync($@"SELECT COUNT(*) FROM `{Fixture.TestStore.Name}.Customers` WHERE `City` = {{0}}", city)
            : context.Database.ExecuteSqlRaw($@"SELECT COUNT(*) FROM `{Fixture.TestStore.Name}.Customers` WHERE `City` = {{0}}", city);

        Assert.Equal(-1, actual);
    }

    [ConditionalTheory]
    [InlineData(false)]
    [InlineData(true)]
    public override async Task Query_with_positional_dbParameter_without_name(bool async)
    {
        var city = CreateDbParameter(name: null, value: "London");

        using var context = CreateContext();

        var actual = async
            ? await context.Database.ExecuteSqlRawAsync($@"SELECT COUNT(*) FROM `{Fixture.TestStore.Name}.Customers` WHERE `City` = {{0}}", city)
            : context.Database.ExecuteSqlRaw($@"SELECT COUNT(*) FROM `{Fixture.TestStore.Name}.Customers` WHERE `City` = {{0}}", city);

        Assert.Equal(-1, actual);
    }

    [ConditionalTheory]
    [InlineData(false)]
    [InlineData(true)]
    public override async Task Query_with_dbParameters_mixed(bool async)
    {
        var city = "London";
        var contactTitle = "Sales Representative";

        var cityParameter = CreateDbParameter("@city", city);
        var contactTitleParameter = CreateDbParameter("@contactTitle", contactTitle);

        using var context = CreateContext();

        var actual = async
            ? await context.Database.ExecuteSqlRawAsync(
                $@"SELECT COUNT(*) FROM `{Fixture.TestStore.Name}.Customers` WHERE `City` = {{0}} AND `ContactTitle` = @contactTitle", city,
                contactTitleParameter)
            : context.Database.ExecuteSqlRaw(
                $@"SELECT COUNT(*) FROM `{Fixture.TestStore.Name}.Customers` WHERE `City` = {{0}} AND `ContactTitle` = @contactTitle", city,
                contactTitleParameter);

        Assert.Equal(-1, actual);

        actual = async
            ? await context.Database.ExecuteSqlRawAsync(
                $@"SELECT COUNT(*) FROM `{Fixture.TestStore.Name}.Customers` WHERE `City` = @city AND `ContactTitle` = {{1}}", cityParameter, contactTitle)
            : context.Database.ExecuteSqlRaw(
                $@"SELECT COUNT(*) FROM `{Fixture.TestStore.Name}.Customers` WHERE `City` = @city AND `ContactTitle` = {{1}}", cityParameter, contactTitle);

        Assert.Equal(-1, actual);
    }

    [ConditionalTheory]
    [InlineData(false)]
    [InlineData(true)]
    public override async Task Query_with_parameters_interpolated(bool async)
    {
        var city = "London";
        var contactTitle = "Sales Representative";

        using var context = CreateContext();

        var tableName = $"`{Fixture.TestStore.Name}.Customers`";
        var sql = FormattableStringFactory.Create(
            "SELECT COUNT(*) FROM " + tableName + " WHERE `City` = {0} AND `ContactTitle` = {1}",
            city, contactTitle);

        var actual = async
            ? await context.Database.ExecuteSqlInterpolatedAsync(sql)
            : context.Database.ExecuteSqlInterpolated(sql);

        Assert.Equal(-1, actual);
    }

    [ConditionalTheory]
    [InlineData(false)]
    [InlineData(true)]
    public override async Task Query_with_DbParameters_interpolated(bool async)
    {
        var city = CreateDbParameter("city", "London");
        var contactTitle = CreateDbParameter("contactTitle", "Sales Representative");

        using var context = CreateContext();

        var tableName = $"`{Fixture.TestStore.Name}.Customers`";
        var sql = FormattableStringFactory.Create(
            "SELECT COUNT(*) FROM " + tableName + " WHERE `City` = {0} AND `ContactTitle` = {1}",
            city, contactTitle);

        var actual = async
            ? await context.Database.ExecuteSqlInterpolatedAsync(sql)
            : context.Database.ExecuteSqlInterpolated(sql);

        Assert.Equal(-1, actual);
    }

    [ConditionalTheory]
    [InlineData(false)]
    [InlineData(true)]
    public override async Task Query_with_parameters_interpolated_2(bool async)
    {
        var city = "London";
        var contactTitle = "Sales Representative";

        using var context = CreateContext();

        var tableName = $"`{Fixture.TestStore.Name}.Customers`";
        var sql = FormattableStringFactory.Create(
            "SELECT COUNT(*) FROM " + tableName + " WHERE `City` = {0} AND `ContactTitle` = {1}",
            city, contactTitle);

        var actual = async
            ? await context.Database.ExecuteSqlAsync(sql)
            : context.Database.ExecuteSql(sql);

        Assert.Equal(-1, actual);
    }

    [ConditionalTheory]
    [InlineData(false)]
    [InlineData(true)]
    public override async Task Query_with_DbParameters_interpolated_2(bool async)
    {
        var city = CreateDbParameter("city", "London");
        var contactTitle = CreateDbParameter("contactTitle", "Sales Representative");

        using var context = CreateContext();

        var tableName = $"`{Fixture.TestStore.Name}.Customers`";
        var sql = FormattableStringFactory.Create(
            "SELECT COUNT(*) FROM " + tableName + " WHERE `City` = {0} AND `ContactTitle` = {1}",
            city, contactTitle);

        var actual = async
            ? await context.Database.ExecuteSqlAsync(sql)
            : context.Database.ExecuteSql(sql);

        Assert.Equal(-1, actual);
    }

    [ConditionalTheory]
    [InlineData(false)]
    [InlineData(true)]
    public override async Task Query_with_parameters_custom_converter(bool async)
    {
        var city = new Microsoft.EntityFrameworkCore.TestModels.ConcurrencyModel.City { Name = "London" };
        var contactTitle = "Sales Representative";

        using var context = CreateContext();

        var tableName = $"`{Fixture.TestStore.Name}.Customers`";
        var sql = FormattableStringFactory.Create(
            "SELECT COUNT(*) FROM " + tableName + " WHERE `City` = {0} AND `ContactTitle` = {1}",
            city, contactTitle);

        var actual = async
            ? await context.Database.ExecuteSqlAsync(sql)
            : context.Database.ExecuteSql(sql);

        Assert.Equal(-1, actual);
    }

    private static Microsoft.EntityFrameworkCore.TestModels.Northwind.Customer Process(Microsoft.EntityFrameworkCore.TestModels.Northwind.Customer c, ManualResetEventSlim e, SemaphoreSlim s)
    {
        e.Set();
        s.Wait();
        s.Release(1);
        return c;
    }
}