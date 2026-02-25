using Ivy.EntityFrameworkCore.BigQuery.TestUtilities;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit.Abstractions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query;

public class OwnedQueryBigQueryTest : OwnedQueryRelationalTestBase<OwnedQueryBigQueryTest.OwnedQueryBigQueryFixture>
{
    public OwnedQueryBigQueryTest(OwnedQueryBigQueryFixture fixture, ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
        Fixture.TestSqlLoggerFactory.Clear();
        Fixture.TestSqlLoggerFactory.SetTestOutputHelper(testOutputHelper);
    }

    private void AssertSql(params string[] expected)
        => Fixture.TestSqlLoggerFactory.AssertBaseline(expected);

    #region Unsupported: Correlated subqueries with LIMIT/OFFSET

    [ConditionalTheory(Skip = "BigQuery does not support correlated subqueries with LIMIT/OFFSET")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Navigation_rewrite_on_owned_collection_with_composition(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support correlated subqueries with LIMIT/OFFSET")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Navigation_rewrite_on_owned_collection_with_composition_complex(bool async)
        => Task.CompletedTask;

    #endregion

    public class OwnedQueryBigQueryFixture : RelationalOwnedQueryFixture
    {
        protected override ITestStoreFactory TestStoreFactory
            => BigQueryTestStoreFactory.Instance;
    }
}
