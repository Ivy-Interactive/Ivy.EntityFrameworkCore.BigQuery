using Ivy.EntityFrameworkCore.BigQuery.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.BulkUpdates;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.TestModels.ComplexTypeModel;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit.Abstractions;

namespace Ivy.EntityFrameworkCore.BigQuery.Update;
public class ComplexTypeBulkUpdatesBigQueryTest(
ComplexTypeBulkUpdatesBigQueryTest.ComplexTypeBulkUpdatesBigQueryFixture fixture,
ITestOutputHelper testOutputHelper)
: ComplexTypeBulkUpdatesRelationalTestBase<ComplexTypeBulkUpdatesBigQueryTest.ComplexTypeBulkUpdatesBigQueryFixture>(fixture, testOutputHelper)
{

    private void AssertExecuteUpdateSql(params string[] expected)
    => Fixture.TestSqlLoggerFactory.AssertBaseline(expected, forUpdate: true);

    private void AssertSql(params string[] expected)
        => Fixture.TestSqlLoggerFactory.AssertBaseline(expected);

    protected void ClearLog()
        => Fixture.TestSqlLoggerFactory.Clear();

    public class ComplexTypeBulkUpdatesBigQueryFixture : ComplexTypeBulkUpdatesRelationalFixtureBase
    {
        private bool _needsReseed;

        protected override ITestStoreFactory TestStoreFactory
            => BigQueryTestStoreFactory.Instance;

        public override void UseTransaction(DatabaseFacade facade, IDbContextTransaction transaction)
        {
            // BigQuery doesn't support transaction rollback, so we need to clean
            // and reseed data before each test to ensure consistent test state.
            if (_needsReseed)
            {
                // Fast reseed: delete from known entity sets and re-seed
                using var context = CreateContext();
                context.Set<CustomerGroup>().ExecuteDelete();
                context.Set<Customer>().ExecuteDelete();
                context.Set<ValuedCustomerGroup>().ExecuteDelete();
                context.Set<ValuedCustomer>().ExecuteDelete();
                ComplexTypeData.SeedAsync(context).GetAwaiter().GetResult();
            }
            _needsReseed = true;
        }
    }
}