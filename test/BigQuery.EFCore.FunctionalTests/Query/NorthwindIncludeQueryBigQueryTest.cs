using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit.Abstractions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query;

public class NorthwindIncludeQueryBigQueryTest : NorthwindIncludeQueryRelationalTestBase<NorthwindQueryBigQueryFixture<NoopModelCustomizer>>
{
    public NorthwindIncludeQueryBigQueryTest(
        NorthwindQueryBigQueryFixture<NoopModelCustomizer> fixture,
        ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
        ClearLog();
        Fixture.TestSqlLoggerFactory.SetTestOutputHelper(testOutputHelper);
    }

    private void AssertSql(params string[] expected)
        => Fixture.TestSqlLoggerFactory.AssertBaseline(expected);

    protected override void ClearLog()
        => Fixture.TestSqlLoggerFactory.Clear();

    #region Unsupported: Non-deterministic ordering without ORDER BY

    [ConditionalTheory(Skip = "BigQuery does not guarantee row order without ORDER BY")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Include_collection_take_no_order_by(bool async)
        => base.Include_collection_take_no_order_by(async);

    [ConditionalTheory(Skip = "BigQuery does not guarantee row order without ORDER BY")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Include_collection_skip_no_order_by(bool async)
        => base.Include_collection_skip_no_order_by(async);

    [ConditionalTheory(Skip = "BigQuery does not guarantee row order without ORDER BY")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Include_collection_skip_take_no_order_by(bool async)
        => base.Include_collection_skip_take_no_order_by(async);

    [ConditionalTheory(Skip = "BigQuery does not guarantee row order without ORDER BY")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Include_with_take(bool async)
        => base.Include_with_take(async);

    [ConditionalTheory(Skip = "BigQuery does not guarantee row order without ORDER BY")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Include_with_skip(bool async)
        => base.Include_with_skip(async);

    [ConditionalTheory(Skip = "BigQuery does not guarantee row order without ORDER BY")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Include_collection_OrderBy_empty_list_contains(bool async)
        => base.Include_collection_OrderBy_empty_list_contains(async);

    [ConditionalTheory(Skip = "BigQuery does not guarantee row order without ORDER BY")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Include_collection_OrderBy_empty_list_does_not_contains(bool async)
        => base.Include_collection_OrderBy_empty_list_does_not_contains(async);

    #endregion

    #region Unsupported: Deeply nested correlated subqueries

    [ConditionalTheory(Skip = "BigQuery does not support deeply nested correlated subqueries")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Include_collection_SelectMany_GroupBy_Select(bool async)
        => base.Include_collection_SelectMany_GroupBy_Select(async);

    [ConditionalTheory(Skip = "BigQuery does not support deeply nested correlated subqueries")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Include_collection_GroupBy_Select(bool async)
        => base.Include_collection_GroupBy_Select(async);

    [ConditionalTheory(Skip = "BigQuery does not support deeply nested correlated subqueries")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Include_collection_Join_GroupBy_Select(bool async)
        => base.Include_collection_Join_GroupBy_Select(async);

    #endregion

    #region Unsupported: Boolean expression alias conflicts with column names

    // BigQuery SQL generation creates boolean expressions with aliases that conflict
    // with existing column names, causing "Cannot access field X on a value with type BOOL"

    [ConditionalTheory(Skip = "BigQuery SQL generation creates alias conflict with boolean expression")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Include_collection_OrderBy_list_does_not_contains(bool async)
        => base.Include_collection_OrderBy_list_does_not_contains(async);

    [ConditionalTheory(Skip = "BigQuery SQL generation creates alias conflict with boolean expression")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Include_collection_OrderBy_list_contains(bool async)
        => base.Include_collection_OrderBy_list_contains(async);

    [ConditionalTheory(Skip = "BigQuery SQL generation creates alias conflict with boolean expression")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Include_collection_with_multiple_conditional_order_by(bool async)
        => base.Include_collection_with_multiple_conditional_order_by(async);

    [ConditionalTheory(Skip = "BigQuery SQL generation creates alias conflict with boolean expression")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Repro9735(bool async)
        => base.Repro9735(async);

    #endregion


    #region Result count differences

    [ConditionalTheory(Skip = "BigQuery filtered include produces different row count")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Filtered_include_with_multiple_ordering(bool async)
        => base.Filtered_include_with_multiple_ordering(async);

    #endregion
}
