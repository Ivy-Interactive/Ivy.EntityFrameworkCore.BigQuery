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
}
