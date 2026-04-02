using Ivy.EntityFrameworkCore.BigQuery.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestModels.FunkyDataModel;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit.Abstractions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query;

public class FunkyDataQueryBigQueryTest : FunkyDataQueryTestBase<FunkyDataQueryBigQueryTest.FunkyDataQueryBigQueryFixture>
{
    // ReSharper disable once UnusedParameter.Local
    public FunkyDataQueryBigQueryTest(FunkyDataQueryBigQueryFixture fixture, ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
        Fixture.TestSqlLoggerFactory.Clear();
        Fixture.TestSqlLoggerFactory.SetTestOutputHelper(testOutputHelper);
    }

    // BigQuery doesn't support reading an empty string as a char at the ADO level
    public override Task String_FirstOrDefault_and_LastOrDefault(bool async)
        => Task.CompletedTask;

    private void AssertSql(params string[] expected)
        => Fixture.TestSqlLoggerFactory.AssertBaseline(expected);

    public class FunkyDataQueryBigQueryFixture : FunkyDataQueryFixtureBase, ITestSqlLoggerFactory
    {
        private FunkyDataData? _expectedData;

        public TestSqlLoggerFactory TestSqlLoggerFactory
            => (TestSqlLoggerFactory)ListLoggerFactory;

        protected override ITestStoreFactory TestStoreFactory
            => BigQueryTestStoreFactory.Instance;

        public override FunkyDataContext CreateContext()
        {
            var context = base.CreateContext();
            context.ChangeTracker.QueryTrackingBehavior = QueryTrackingBehavior.NoTracking;
            return context;
        }

        public override ISetSource GetExpectedData()
        {
            return _expectedData ??= (FunkyDataData)base.GetExpectedData();
        }

        protected override async Task SeedAsync(FunkyDataContext context)
        {
            context.FunkyCustomers.AddRange(GetExpectedData().Set<FunkyCustomer>());
            await context.SaveChangesAsync();
        }
    }
}