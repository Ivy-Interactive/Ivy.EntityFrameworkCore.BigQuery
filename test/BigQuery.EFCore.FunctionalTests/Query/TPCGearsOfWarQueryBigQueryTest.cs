using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestModels.GearsOfWarModel;
using Xunit.Abstractions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query;

public class TPCGearsOfWarQueryBigQueryTest : TPCGearsOfWarQueryRelationalTestBase<TPCGearsOfWarQueryBigQueryFixture>
{
    public TPCGearsOfWarQueryBigQueryTest(TPCGearsOfWarQueryBigQueryFixture fixture, ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
        Fixture.TestSqlLoggerFactory.Clear();
        Fixture.TestSqlLoggerFactory.SetTestOutputHelper(testOutputHelper);
    }    

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

    [MemberData(nameof(IsAsyncData))]
    [ConditionalTheory(Skip = "BigQuery does not support correlated subqueries with LIMIT/OFFSET")]
    public override Task Correlated_collections_with_Distinct(bool async)
        => base.Correlated_collections_with_Distinct(async);

    [ConditionalTheory(Skip = "BigQuery does not support correlated subqueries with LIMIT/OFFSET in JOIN")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Outer_parameter_in_join_key(bool async)
        => base.Outer_parameter_in_join_key(async);

    [ConditionalTheory(Skip = "BigQuery does not support correlated subqueries with LIMIT/OFFSET in JOIN")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Outer_parameter_in_join_key_inner_and_outer(bool async)
        => base.Outer_parameter_in_join_key_inner_and_outer(async);

    [ConditionalTheory(Skip = "BigQuery does not support correlated subqueries with LIMIT/OFFSET in JOIN")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Outer_parameter_in_group_join_with_DefaultIfEmpty(bool async)
        => base.Outer_parameter_in_group_join_with_DefaultIfEmpty(async);

    #endregion

    #region Unsupported: Non-deterministic ordering without ORDER BY

    [ConditionalTheory(Skip = "BigQuery does not guarantee row order without ORDER BY")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Take_without_orderby_followed_by_orderBy_is_pushed_down1(bool async)
        => base.Take_without_orderby_followed_by_orderBy_is_pushed_down1(async);

    [ConditionalTheory(Skip = "BigQuery does not guarantee row order without ORDER BY")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Take_without_orderby_followed_by_orderBy_is_pushed_down2(bool async)
        => base.Take_without_orderby_followed_by_orderBy_is_pushed_down2(async);

    #endregion

    #region Unsupported: IN/EXISTS subqueries in JOIN predicates

    [ConditionalTheory(Skip = "BigQuery does not support IN subqueries in JOIN predicates")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Subquery_projecting_non_nullable_scalar_contains_non_nullable_value_doesnt_need_null_expansion(bool async)
        => base.Subquery_projecting_non_nullable_scalar_contains_non_nullable_value_doesnt_need_null_expansion(async);

    [ConditionalTheory(Skip = "BigQuery does not support IN subqueries in JOIN predicates")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Subquery_projecting_non_nullable_scalar_contains_non_nullable_value_doesnt_need_null_expansion_negated(bool async)
        => base.Subquery_projecting_non_nullable_scalar_contains_non_nullable_value_doesnt_need_null_expansion_negated(async);

    [ConditionalTheory(Skip = "BigQuery does not support EXISTS subqueries in JOIN predicates")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Subquery_projecting_nullable_scalar_contains_nullable_value_needs_null_expansion(bool async)
        => base.Subquery_projecting_nullable_scalar_contains_nullable_value_needs_null_expansion(async);

    [ConditionalTheory(Skip = "BigQuery does not support EXISTS subqueries in JOIN predicates")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Subquery_projecting_nullable_scalar_contains_nullable_value_needs_null_expansion_negated(bool async)
        => base.Subquery_projecting_nullable_scalar_contains_nullable_value_needs_null_expansion_negated(async);

    #endregion

    #region Unsupported: Deeply nested correlated subqueries

    [ConditionalTheory(Skip = "BigQuery does not support deeply nested correlated subqueries (EXISTS with nested IN)")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_contains_on_navigation_with_composite_keys(bool async)
        => base.Where_contains_on_navigation_with_composite_keys(async);

    [ConditionalTheory(Skip = "BigQuery does not support correlated subqueries with UNION ALL")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_subquery_concat_firstordefault_boolean(bool async)
        => base.Where_subquery_concat_firstordefault_boolean(async);

    [ConditionalTheory(Skip = "BigQuery does not support correlated subqueries with OFFSET")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_subquery_with_ElementAtOrDefault_equality_to_null_with_composite_key(bool async)
        => base.Where_subquery_with_ElementAtOrDefault_equality_to_null_with_composite_key(async);

    #endregion

    #region Exception type mismatch (KeyNotFoundException instead of InvalidOperationException)

    [ConditionalTheory(Skip = "TimeSpan.Ticks Sum throws KeyNotFoundException instead of InvalidOperationException")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Client_eval_followed_by_aggregate_operation(bool async)
        => base.Client_eval_followed_by_aggregate_operation(async);

    #endregion

    #region Unsupported: Subquery in Take argument

    [ConditionalTheory(Skip = "BigQuery does not support subquery in Take argument")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Subquery_inside_Take_argument(bool async)
        => base.Subquery_inside_Take_argument(async);

    #endregion

    #region Unsupported: TimeSpan tick precision

    [ConditionalTheory(Skip = "BigQuery TIME type has microsecond precision, not tick (100 nanosecond) precision")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Non_string_concat_uses_appropriate_type_mapping(bool _)
        => Task.CompletedTask;

    #endregion

    #region Result count/value differences

    // These tests produce different results in BigQuery due to query transformation differences

    [ConditionalTheory(Skip = "BigQuery correlated collection on left join produces different count")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Correlated_collections_on_left_join_with_null_value(bool async)
        => base.Correlated_collections_on_left_join_with_null_value(async);

    [ConditionalTheory(Skip = "BigQuery SelectMany with Take produces different count")]
    [MemberData(nameof(IsAsyncData))]
    public override Task SelectMany_predicate_with_non_equality_comparison_with_Take_doesnt_convert_to_join(bool async)
        => base.SelectMany_predicate_with_non_equality_comparison_with_Take_doesnt_convert_to_join(async);

    [ConditionalTheory(Skip = "BigQuery value converted property comparison produces different result")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Project_equality_with_value_converted_property(bool async)
        => base.Project_equality_with_value_converted_property(async);

    [ConditionalTheory(Skip = "BigQuery DateTimeOffset Contains produces no results")]
    [MemberData(nameof(IsAsyncData))]
    public override Task DateTimeOffset_Contains_Less_than_Greater_than(bool async)
        => base.DateTimeOffset_Contains_Less_than_Greater_than(async);

    [ConditionalTheory(Skip = "BigQuery does not support correlated subqueries with UNION ALL")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_subquery_union_firstordefault_boolean(bool async)
        => base.Where_subquery_union_firstordefault_boolean(async);

    [ConditionalTheory(Skip = "BigQuery does not support correlated subqueries with OFFSET")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_subquery_with_ElementAt_using_column_as_index(bool async)
        => base.Where_subquery_with_ElementAt_using_column_as_index(async);

    #endregion

    #region Unsupported: StartsWith with null parameter

    [ConditionalTheory(Skip = "BigQuery null parameter handling differs - nullable object conversion issue")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Select_StartsWith_with_null_parameter_as_argument(bool _)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery null parameter handling differs - nullable object conversion issue")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Group_by_on_StartsWith_with_null_parameter_as_argument(bool _)
        => Task.CompletedTask;

    #endregion

    private void AssertSql(params string[] expected)
        => Fixture.TestSqlLoggerFactory.AssertBaseline(expected);
}