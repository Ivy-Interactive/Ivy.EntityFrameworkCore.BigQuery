using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit.Abstractions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query;
public class NorthwindJoinQueryBigQueryTest : NorthwindJoinQueryRelationalTestBase<NorthwindQueryBigQueryFixture<NoopModelCustomizer>>
{
    // ReSharper disable once UnusedParameter.Local
    public NorthwindJoinQueryBigQueryTest(NorthwindQueryBigQueryFixture<NoopModelCustomizer> fixture, ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
        ClearLog();
        Fixture.TestSqlLoggerFactory.SetTestOutputHelper(testOutputHelper);
    }

    protected override void ClearLog()
        => Fixture.TestSqlLoggerFactory.Clear();

    #region Unsupported: Correlated subqueries with LIMIT/OFFSET

    [ConditionalTheory(Skip = "BigQuery does not support correlated subqueries with LIMIT/OFFSET")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Take_in_collection_projection_with_FirstOrDefault_on_top_level(bool async)
        => Task.CompletedTask;

    #endregion

    #region Unsupported: Join with local collection (EF Core issue #14672)

    public override async Task Join_local_collection_int_closure_is_cached_correctly(bool async)
    {
        // Join with local collection. Issue #14672.
        await AssertTranslationFailed(() => base.Join_local_collection_int_closure_is_cached_correctly(async));
    }

    #endregion
}
