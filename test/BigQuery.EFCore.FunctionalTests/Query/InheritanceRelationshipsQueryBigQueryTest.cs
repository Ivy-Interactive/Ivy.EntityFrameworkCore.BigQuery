using Ivy.EntityFrameworkCore.BigQuery.TestUtilities;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit.Abstractions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query;

public class InheritanceRelationshipsQueryBigQueryTest
    : InheritanceRelationshipsQueryRelationalTestBase<InheritanceRelationshipsQueryBigQueryTest.InheritanceRelationshipsQueryBigQueryFixture>
{
    // ReSharper disable once UnusedParameter.Local
    public InheritanceRelationshipsQueryBigQueryTest(
        InheritanceRelationshipsQueryBigQueryFixture fixture,
        ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
        Fixture.TestSqlLoggerFactory.Clear();
        Fixture.TestSqlLoggerFactory.SetTestOutputHelper(testOutputHelper);
    }

    private void AssertSql(params string[] expected)
        => Fixture.TestSqlLoggerFactory.AssertBaseline(expected);

    public class InheritanceRelationshipsQueryBigQueryFixture : InheritanceRelationshipsQueryRelationalFixture
    {
        protected override ITestStoreFactory TestStoreFactory
            => BigQueryTestStoreFactory.Instance;
    }
}