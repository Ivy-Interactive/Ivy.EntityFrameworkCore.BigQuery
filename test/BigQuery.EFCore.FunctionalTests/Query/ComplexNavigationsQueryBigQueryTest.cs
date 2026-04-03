using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace Ivy.EntityFrameworkCore.BigQuery.Query;

public class ComplexNavigationsQueryBigQueryTest : ComplexNavigationsQueryRelationalTestBase<ComplexNavigationsQueryBigQueryFixture>
{
    // ReSharper disable once UnusedParameter.Local
    public ComplexNavigationsQueryBigQueryTest(ComplexNavigationsQueryBigQueryFixture fixture, ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
        Fixture.TestSqlLoggerFactory.Clear();
        Fixture.TestSqlLoggerFactory.SetTestOutputHelper(testOutputHelper);
    }

    private void AssertSql(params string[] expected)
        => Fixture.TestSqlLoggerFactory.AssertBaseline(expected);

    public override async Task Join_with_result_selector_returning_queryable_throws_validation_error(bool async)
        => await Assert.ThrowsAsync<ArgumentException>(
            () => base.Join_with_result_selector_returning_queryable_throws_validation_error(async));

    public override Task GroupJoin_client_method_in_OrderBy(bool async)
        => AssertTranslationFailedWithDetails(
            () => base.GroupJoin_client_method_in_OrderBy(async),
            CoreStrings.QueryUnableToTranslateMethod(
                "Microsoft.EntityFrameworkCore.Query.ComplexNavigationsQueryTestBase<Ivy.EntityFrameworkCore.BigQuery.Query.ComplexNavigationsQueryBigQueryFixture>",
                "ClientMethodNullableInt"));

    public override Task Nested_SelectMany_correlated_with_join_table_correctly_translated_to_apply(bool async)
        => Assert.ThrowsAsync<EqualException>(
            async () => await base.Nested_SelectMany_correlated_with_join_table_correctly_translated_to_apply(async));

    [ConditionalTheory(Skip = "Non-deterministic query - ORDER BY only on parent table, child order undefined. SQL Server returns 'L2 02' first, BigQuery returns 'L2 10' first.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task SelectMany_subquery_with_custom_projection(bool _)
        => Task.CompletedTask;

    #region Unsupported: Correlated subquery with LIMIT/OFFSET

    [ConditionalTheory(Skip = "BigQuery does not support correlated subquery with LIMIT/OFFSET pattern")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Single_select_many_in_projection_with_take(bool _)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support correlated subquery with LIMIT/OFFSET pattern")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Multiple_select_many_in_projection(bool _)
        => Task.CompletedTask;

    #endregion
}