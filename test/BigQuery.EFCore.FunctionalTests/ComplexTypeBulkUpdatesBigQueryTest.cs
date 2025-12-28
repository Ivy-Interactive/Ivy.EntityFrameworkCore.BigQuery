using Ivy.EntityFrameworkCore.BigQuery.TestUtilities;
using Microsoft.EntityFrameworkCore.BulkUpdates;
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
        protected override ITestStoreFactory TestStoreFactory
            => BigQueryTestStoreFactory.Instance;
    }

}

