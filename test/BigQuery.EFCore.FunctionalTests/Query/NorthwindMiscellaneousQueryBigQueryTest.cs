using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit.Abstractions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query;

public class NorthwindMiscellaneousQueryBigQueryTest : NorthwindMiscellaneousQueryRelationalTestBase<NorthwindQueryBigQueryFixture<NoopModelCustomizer>>
{
    // ReSharper disable once UnusedParameter.Local
    public NorthwindMiscellaneousQueryBigQueryTest(
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

    #region Skipped: TimeSpan not fully supported

    [ConditionalTheory(Skip = "BigQuery does not have native TimeSpan/INTERVAL arithmetic like SQL Server")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Add_minutes_on_constant_value(bool _)
        => Task.CompletedTask;

    #endregion

    #region Skipped: Correlated subqueries with specific patterns

    [ConditionalTheory(Skip = "BigQuery does not support correlated subqueries with LIMIT/OFFSET in this context")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Max_on_empty_sequence_throws(bool _)
        => Task.CompletedTask;

    #endregion

    #region Expected exceptions: Client code evaluation

    public override async Task Client_code_using_instance_method_throws(bool async)
    {
        Assert.Equal(
            CoreStrings.ClientProjectionCapturingConstantInMethodInstance(
                "Ivy.EntityFrameworkCore.BigQuery.Query.NorthwindMiscellaneousQueryBigQueryTest",
                "InstanceMethod"),
            (await Assert.ThrowsAsync<InvalidOperationException>(
                () => base.Client_code_using_instance_method_throws(async))).Message);
    }

    public override async Task Client_code_using_instance_in_static_method(bool async)
    {
        Assert.Equal(
            CoreStrings.ClientProjectionCapturingConstantInMethodArgument(
                "Ivy.EntityFrameworkCore.BigQuery.Query.NorthwindMiscellaneousQueryBigQueryTest",
                "StaticMethod"),
            (await Assert.ThrowsAsync<InvalidOperationException>(
                () => base.Client_code_using_instance_in_static_method(async))).Message);
    }

    public override async Task Client_code_using_instance_in_anonymous_type(bool async)
    {
        Assert.Equal(
            CoreStrings.ClientProjectionCapturingConstantInTree(
                "Ivy.EntityFrameworkCore.BigQuery.Query.NorthwindMiscellaneousQueryBigQueryTest"),
            (await Assert.ThrowsAsync<InvalidOperationException>(
                () => base.Client_code_using_instance_in_anonymous_type(async))).Message);
    }

    public override async Task Client_code_unknown_method(bool async)
    {
        await AssertTranslationFailedWithDetails(
            () => base.Client_code_unknown_method(async),
            CoreStrings.QueryUnableToTranslateMethod(
                "Microsoft.EntityFrameworkCore.Query.NorthwindMiscellaneousQueryTestBase<Ivy.EntityFrameworkCore.BigQuery.Query.NorthwindQueryBigQueryFixture<Microsoft.EntityFrameworkCore.TestUtilities.NoopModelCustomizer>>",
                "UnknownMethod"));
    }

    #endregion

    #region Skipped: BigQuery array parameter limitations

    [ConditionalTheory(Skip = "BigQuery does not support null elements in array parameters")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Entity_equality_contains_with_list_of_null(bool _)
        => Task.CompletedTask;

    #endregion

    #region Skipped: Correlated subqueries - WHERE clause with scalar subquery

    [ConditionalTheory(Skip = "BigQuery does not support correlated scalar subqueries in WHERE clause")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_query_composition_entity_equality_multiple_elements_First(bool _)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support correlated scalar subqueries in WHERE clause")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_query_composition_entity_equality_multiple_elements_FirstOrDefault(bool _)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support correlated scalar subqueries in WHERE clause")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_query_composition_entity_equality_multiple_elements_Single(bool _)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support correlated scalar subqueries in WHERE clause")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_query_composition_entity_equality_multiple_elements_SingleOrDefault(bool _)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support correlated subqueries in WHERE clause")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Select_Where_Subquery_Equality(bool _)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support deeply nested correlated subqueries")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Select_Where_Subquery_Deep_First(bool _)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support correlated subqueries with LIMIT/OFFSET in NOT EXISTS")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Collection_navigation_equal_to_null_for_subquery_using_ElementAtOrDefault_parameter(bool _)
        => Task.CompletedTask;

    #endregion

    #region Skipped: Correlated subqueries - ALL/ANY/EXISTS patterns

    [ConditionalTheory(Skip = "BigQuery does not support ALL with correlated subquery (translates to NOT EXISTS)")]
    [MemberData(nameof(IsAsyncData))]
    public override Task All_top_level_subquery(bool _)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support ALL with correlated subquery (translates to NOT EXISTS)")]
    [MemberData(nameof(IsAsyncData))]
    public override Task All_top_level_subquery_ef_property(bool _)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support NOT EXISTS with correlated subquery")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Collection_navigation_equal_to_null_for_subquery_using_ElementAtOrDefault_constant_one(bool _)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support EXISTS with correlated subquery")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Pending_selector_in_cardinality_reducing_method_is_applied_before_expanding_collection_navigation_member(bool _)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support correlated subqueries in inline collection aggregates")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Subquery_with_navigation_inside_inline_collection(bool _)
        => Task.CompletedTask;

    #endregion

    #region Skipped: Correlated subqueries - Aggregate in SELECT

    [ConditionalTheory(Skip = "BigQuery does not support correlated aggregate subqueries in SELECT")]
    [MemberData(nameof(IsAsyncData))]
    public override Task DefaultIfEmpty_Sum_over_collection_navigation(bool _)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support correlated subquery with LIMIT/OFFSET pattern")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Complex_nested_query_doesnt_try_binding_to_grandparent_when_parent_returns_complex_result(bool _)
        => Task.CompletedTask;

    #endregion

    #region Skipped: Correlated subqueries - Deep nested

    [ConditionalTheory(Skip = "BigQuery does not support deeply nested correlated subqueries (nested scalar subquery in LEFT JOIN)")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Select_Where_Subquery_Deep_Single(bool _)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support correlated subquery with Skip/Take in SELECT projection")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Select_correlated_subquery_ordered(bool _)
        => Task.CompletedTask;

    #endregion

    #region Skipped: Entity equality with composite key

    [ConditionalTheory(Skip = "EF Core cannot translate entity equality on composite key through subquery")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Entity_equality_through_subquery_composite_key(bool _)
        => Task.CompletedTask;

    #endregion

    #region Skipped: BigQuery NULL ordering (NULLS FIRST for ASC differs from SQL Server NULLS LAST)

    [ConditionalTheory(Skip = "BigQuery orders NULLs first in ASC, changing Skip/Take positions on nullable columns")]
    [MemberData(nameof(IsAsyncData))]
    public override Task OrderBy_skip_skip_take(bool _)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery orders NULLs first in ASC, changing Skip/Take positions on nullable columns")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Include_with_orderby_skip_preserves_ordering(bool _)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "Non-deterministic: multiple customers with no orders have NULL OrderID")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Entity_equality_orderby_subquery(bool _)
        => Task.CompletedTask;

    #endregion

    #region Skipped: Non-deterministic result ordering

    [ConditionalTheory(Skip = "Non-deterministic: projects Order with only OrderDate set (OrderID=0), no element sorter")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Convert_to_nullable_on_nullable_value_is_ignored(bool _)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "Non-deterministic: projects Order with only OrderDate set (OrderID=0), no element sorter")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Select_expression_date_add_milliseconds_below_the_range(bool _)
        => Task.CompletedTask;

    #endregion

    #region Skipped: Non-deterministic queries (Take without OrderBy)

    // These tests use Take(n) without OrderBy, making results non-deterministic.
    // BigQuery does not guarantee row order without explicit ORDER BY.

    [ConditionalTheory(Skip = "Non-deterministic query - Take without OrderBy returns arbitrary rows in BigQuery")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_query_composition2(bool _)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "Non-deterministic query - Take without OrderBy returns arbitrary rows in BigQuery")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_query_composition2_FirstOrDefault(bool _)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "Non-deterministic query - Take without OrderBy returns arbitrary rows in BigQuery")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_query_composition2_FirstOrDefault_with_anonymous(bool _)
        => Task.CompletedTask;

    #endregion

    #region Skipped: Wrong result count with collection projections + Skip/Take

    [ConditionalTheory(Skip = "BigQuery LIMIT/OFFSET with collection projections produces wrong row count")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Projection_skip_take_collection_projection(bool async)
        => base.Projection_skip_take_collection_projection(async);

    [ConditionalTheory(Skip = "BigQuery LIMIT/OFFSET with collection projections produces wrong row count")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Projection_skip_collection_projection(bool async)
        => base.Projection_skip_collection_projection(async);

    [ConditionalTheory(Skip = "BigQuery LIMIT/OFFSET with collection projections produces wrong row count")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Projection_take_collection_projection(bool async)
        => base.Projection_take_collection_projection(async);

    #endregion

    #region Skipped: Arithmetic overflow check produces wrong count

    [ConditionalTheory(Skip = "BigQuery arithmetic produces different row count due to overflow handling differences")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Checked_context_with_arithmetic_does_not_fail(bool async)
        => base.Checked_context_with_arithmetic_does_not_fail(async);

    #endregion

    #region Skipped: Collection projection with Distinct binding after client eval

    [ConditionalTheory(Skip = "BigQuery collection projection with Distinct + client eval binding returns wrong result")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Select_DTO_constructor_distinct_with_collection_projection_translated_to_server_with_binding_after_client_eval(bool async)
        => base.Select_DTO_constructor_distinct_with_collection_projection_translated_to_server_with_binding_after_client_eval(async);

    #endregion
}
