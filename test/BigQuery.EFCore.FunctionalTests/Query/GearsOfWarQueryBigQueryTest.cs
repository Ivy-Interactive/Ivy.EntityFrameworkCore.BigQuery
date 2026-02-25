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
SELECT m.`Timeline` > CURRENT_DATETIME()
FROM `Missions` AS m
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
}