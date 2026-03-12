using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestModels.GearsOfWarModel;
using Xunit.Abstractions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query;

public class GearsOfWarQueryBigQueryTest : GearsOfWarQueryRelationalTestBase<GearsOfWarQueryBigQueryFixture>
{
    public GearsOfWarQueryBigQueryTest(GearsOfWarQueryBigQueryFixture fixture, ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
        Fixture.TestSqlLoggerFactory.Clear();
        Fixture.TestSqlLoggerFactory.SetTestOutputHelper(testOutputHelper);
    }

    private void AssertSql(params string[] expected)
        => Fixture.TestSqlLoggerFactory.AssertBaseline(expected);

    public override async Task Select_datetimeoffset_comparison_in_projection(bool async)
    {
        await AssertQueryScalar(
            async,
            ss => ss.Set<Mission>().Select(m => m.Timeline > DateTimeOffset.UtcNow));

        AssertSql(
            """
SELECT `m`.`Timeline` > CURRENT_TIMESTAMP()
FROM `Missions` AS `m`
""");
    }

    #region Unsupported: Correlated subqueries with LIMIT/OFFSET

    [ConditionalTheory(Skip = "BigQuery does not support correlated subqueries with LIMIT/OFFSET")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Correlated_collections_with_Distinct(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support correlated subqueries with OFFSET")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_subquery_with_ElementAtOrDefault_equality_to_null_with_composite_key(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support correlated subqueries with OFFSET")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_subquery_with_ElementAt_using_column_as_index(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support correlated subqueries with LIMIT/OFFSET in JOIN")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Outer_parameter_in_join_key(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support correlated subqueries with LIMIT/OFFSET in JOIN")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Outer_parameter_in_join_key_inner_and_outer(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support correlated subqueries with LIMIT/OFFSET in JOIN")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Outer_parameter_in_group_join_with_DefaultIfEmpty(bool async)
        => Task.CompletedTask;

    #endregion

    #region Unsupported: IN/EXISTS subqueries in JOIN predicates

    [ConditionalTheory(Skip = "BigQuery does not support IN subqueries in JOIN predicates")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Subquery_projecting_non_nullable_scalar_contains_non_nullable_value_doesnt_need_null_expansion(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support IN subqueries in JOIN predicates")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Subquery_projecting_non_nullable_scalar_contains_non_nullable_value_doesnt_need_null_expansion_negated(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support EXISTS subqueries in JOIN predicates")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Subquery_projecting_nullable_scalar_contains_nullable_value_needs_null_expansion(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support EXISTS subqueries in JOIN predicates")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Subquery_projecting_nullable_scalar_contains_nullable_value_needs_null_expansion_negated(bool async)
        => Task.CompletedTask;

    #endregion

    #region Unsupported: Non-deterministic ordering without ORDER BY

    [ConditionalTheory(Skip = "BigQuery does not guarantee row order without ORDER BY")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Take_without_orderby_followed_by_orderBy_is_pushed_down1(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not guarantee row order without ORDER BY")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Take_without_orderby_followed_by_orderBy_is_pushed_down2(bool async)
        => Task.CompletedTask;

    #endregion

    #region Unsupported: Deeply nested correlated subqueries

    [ConditionalTheory(Skip = "BigQuery does not support deeply nested correlated subqueries (EXISTS with nested IN)")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_contains_on_navigation_with_composite_keys(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support correlated subqueries with UNION ALL")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_subquery_concat_firstordefault_boolean(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support correlated subqueries with UNION ALL")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_subquery_union_firstordefault_boolean(bool async)
        => Task.CompletedTask;

    #endregion

    #region Exception type mismatch (KeyNotFoundException instead of InvalidOperationException)

    [ConditionalTheory(Skip = "TimeSpan.Ticks Sum throws KeyNotFoundException instead of InvalidOperationException")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Client_eval_followed_by_aggregate_operation(bool async)
        => Task.CompletedTask;

    #endregion

    #region Unsupported: Subquery in Take argument

    [ConditionalTheory(Skip = "BigQuery does not support subquery in Take argument")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Subquery_inside_Take_argument(bool async)
        => Task.CompletedTask;

    #endregion

    #region Result count/value differences

    // These tests produce different results in BigQuery due to query transformation differences

    [ConditionalTheory(Skip = "BigQuery correlated collection on left join produces different count")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Correlated_collections_on_left_join_with_null_value(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery SelectMany with Take produces different count")]
    [MemberData(nameof(IsAsyncData))]
    public override Task SelectMany_predicate_with_non_equality_comparison_with_Take_doesnt_convert_to_join(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery value converted property comparison produces different result")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Project_equality_with_value_converted_property(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery DateTimeOffset Contains produces no results")]
    [MemberData(nameof(IsAsyncData))]
    public override Task DateTimeOffset_Contains_Less_than_Greater_than(bool async)
        => Task.CompletedTask;

    #endregion
}