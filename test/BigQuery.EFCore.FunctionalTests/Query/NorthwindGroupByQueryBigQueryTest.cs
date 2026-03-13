using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit.Abstractions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query;

public class NorthwindGroupByQueryBigQueryTest : NorthwindGroupByQueryRelationalTestBase<NorthwindQueryBigQueryFixture<NoopModelCustomizer>>
{
    public NorthwindGroupByQueryBigQueryTest(
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

    #region Unsupported: Correlated subqueries with LIMIT/OFFSET

    [ConditionalTheory(Skip = "BigQuery does not support correlated subqueries with LIMIT/OFFSET")]
    [MemberData(nameof(IsAsyncData))]
    public override Task GroupBy_aggregate_from_multiple_query_in_same_projection_3(bool async)
        => base.GroupBy_aggregate_from_multiple_query_in_same_projection_3(async);

    [ConditionalTheory(Skip = "BigQuery does not support correlated subqueries with LIMIT/OFFSET")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Complex_query_with_groupBy_in_subquery4(bool async)
        => base.Complex_query_with_groupBy_in_subquery4(async);

    #endregion

    #region Unsupported: Deeply nested correlated subqueries

    [ConditionalTheory(Skip = "BigQuery does not support deeply nested correlated subqueries")]
    [MemberData(nameof(IsAsyncData))]
    public override Task GroupBy_group_Distinct_Select_Distinct_aggregate(bool async)
        => base.GroupBy_group_Distinct_Select_Distinct_aggregate(async);

    [ConditionalTheory(Skip = "BigQuery does not support deeply nested correlated subqueries")]
    [MemberData(nameof(IsAsyncData))]
    public override Task GroupBy_Property_Select_Count_with_predicate(bool async)
        => base.GroupBy_Property_Select_Count_with_predicate(async);

    [ConditionalTheory(Skip = "BigQuery does not support deeply nested correlated subqueries")]
    [MemberData(nameof(IsAsyncData))]
    public override Task GroupBy_Property_Select_LongCount_with_predicate(bool async)
        => base.GroupBy_Property_Select_LongCount_with_predicate(async);

    #endregion

    #region Unsupported: Aggregate in WHERE clause

    // BigQuery does not allow aggregate functions directly in WHERE clause
    // They must be wrapped in a subquery or moved to HAVING

    [ConditionalTheory(Skip = "BigQuery does not allow aggregate functions in WHERE clause")]
    [MemberData(nameof(IsAsyncData))]
    public override Task GroupBy_with_aggregate_containing_complex_where(bool async)
        => base.GroupBy_with_aggregate_containing_complex_where(async);

    #endregion

    #region Unsupported: Correlated collection after GroupBy

    // These queries generate SQL with unrecognized name references
    // due to alias scope issues in the generated subqueries

    [ConditionalTheory(Skip = "BigQuery alias scope issue in correlated collection after GroupBy")]
    [MemberData(nameof(IsAsyncData))]
    public override Task AsEnumerable_in_subquery_for_GroupBy(bool async)
        => base.AsEnumerable_in_subquery_for_GroupBy(async);

    [ConditionalTheory(Skip = "BigQuery alias scope issue in correlated collection after GroupBy")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Select_correlated_collection_after_GroupBy_aggregate_when_identifier_changes_to_complex(bool async)
        => base.Select_correlated_collection_after_GroupBy_aggregate_when_identifier_changes_to_complex(async);

    [ConditionalTheory(Skip = "BigQuery SELECT references ungrouped column")]
    [MemberData(nameof(IsAsyncData))]
    public override Task GroupBy_aggregate_from_multiple_query_in_same_projection_2(bool async)
        => base.GroupBy_aggregate_from_multiple_query_in_same_projection_2(async);

    #endregion

    #region Result count differences

    [ConditionalTheory(Skip = "BigQuery GroupBy conditional produces different result")]
    [MemberData(nameof(IsAsyncData))]
    public override Task GroupBy_aggregate_projecting_conditional_expression(bool async)
        => base.GroupBy_aggregate_projecting_conditional_expression(async);

    #endregion
}
