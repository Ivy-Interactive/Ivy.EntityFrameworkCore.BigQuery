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
SELECT `m`.`Timeline` > CURRENT_TIMESTAMP
FROM `Missions` AS `m`
""");
    }

    [MemberData(nameof(IsAsyncData))]
    [ConditionalTheory(Skip = "BigQuery does not support correlated subqueries with LIMIT/OFFSET")]
    public override Task Correlated_collections_with_Distinct(bool async)
        => base.Correlated_collections_with_Distinct(async);

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

    [ConditionalTheory(Skip = "BigQuery does not support correlated subqueries with OFFSET")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_subquery_with_ElementAtOrDefault_equality_to_null_with_composite_key(bool async)
        => Task.CompletedTask;

    #endregion

    private void AssertSql(params string[] expected)
        => Fixture.TestSqlLoggerFactory.AssertBaseline(expected);
}