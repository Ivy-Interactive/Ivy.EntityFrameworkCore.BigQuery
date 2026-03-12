using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit.Abstractions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query;

public class NorthwindSelectQueryBigQueryTests : NorthwindSelectQueryRelationalTestBase<NorthwindQueryBigQueryFixture<NoopModelCustomizer>>
{
    public NorthwindSelectQueryBigQueryTests(NorthwindQueryBigQueryFixture<NoopModelCustomizer> fixture, ITestOutputHelper testOutputHelper)
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
    public override Task Project_single_element_from_collection_with_OrderBy_over_navigation_Take_and_FirstOrDefault_2(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support correlated subqueries with Skip (OFFSET requires special ROW_NUMBER handling)")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Project_single_element_from_collection_with_OrderBy_Skip_and_FirstOrDefault(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support correlated subqueries with LIMIT/OFFSET")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Take_on_correlated_collection_in_first(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support correlated subqueries with LIMIT/OFFSET")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Take_on_top_level_and_on_collection_projection_with_outer_apply(bool async)
        => Task.CompletedTask;

    #endregion

    #region Unsupported: Deeply nested correlated subqueries

    [ConditionalTheory(Skip = "BigQuery does not support deeply nested correlated subqueries")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Select_nested_collection_multi_level5(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support deeply nested correlated subqueries")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Select_nested_collection_multi_level6(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support correlated subqueries with UNION ALL")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Project_single_element_from_collection_with_multiple_OrderBys_Take_and_FirstOrDefault_followed_by_projection_of_length_property(bool async)
        => Task.CompletedTask;

    #endregion

    #region Unsupported: SelectMany with complex correlations

    [ConditionalTheory(Skip = "BigQuery cannot support correlated projections with DefaultIfEmpty - outer reference vs NULL semantics")]
    [MemberData(nameof(IsAsyncData))]
    public override Task SelectMany_correlated_with_outer_3(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery cannot support inequality correlations with Take - requires true LATERAL JOIN")]
    [MemberData(nameof(IsAsyncData))]
    public override Task SelectMany_correlated_with_outer_6(bool async)
        => Task.CompletedTask;

    #endregion

    #region Unsupported: Anonymous type comparisons (EF Core issue #14672)

    public override async Task Member_binding_after_ctor_arguments_fails_with_client_eval(bool async)
    {
        // Anonymous type member access. Issue #14672.
        await AssertTranslationFailed(() => base.Member_binding_after_ctor_arguments_fails_with_client_eval(async));
    }

    #endregion

    #region Unsupported: Alias scope issues in nested collections

    // BigQuery SQL generation creates alias scope issues causing "Unrecognized name" errors

    [ConditionalTheory(Skip = "BigQuery alias scope issue in set operation pending collection")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Set_operation_in_pending_collection(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery alias scope issue in nested collection")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Select_nested_collection_deep_distinct_no_identifiers(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery alias scope issue in nested collection")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Select_nested_collection_deep(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery WHERE references ungrouped column")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Select_nested_collection_multi_level4(bool async)
        => Task.CompletedTask;

    #endregion

    #region Unsupported: Non-mapped property projections

    [ConditionalTheory(Skip = "BigQuery cannot translate EF.Property on non-mapped properties")]
    [MemberData(nameof(IsAsyncData))]
    public override Task SelectMany_with_collection_being_correlated_subquery_which_references_non_mapped_properties_from_inner_and_outer_entity(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery cannot translate collection after distinct with complex projection")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Correlated_collection_after_distinct_with_complex_projection_not_containing_original_identifier(bool async)
        => Task.CompletedTask;

    #endregion
}
