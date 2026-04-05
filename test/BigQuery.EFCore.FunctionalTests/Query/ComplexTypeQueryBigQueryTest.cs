using Ivy.EntityFrameworkCore.BigQuery.TestUtilities;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit.Abstractions;

#pragma warning disable xUnit1026 // Unused parameters in skipped test overrides

namespace Ivy.EntityFrameworkCore.BigQuery.Query;

public class ComplexTypeQueryBigQueryTest : ComplexTypeQueryRelationalTestBase<ComplexTypeQueryBigQueryTest.ComplexTypeQueryBigQueryFixture>
{
    // ReSharper disable once UnusedParameter.Local
    public ComplexTypeQueryBigQueryTest(ComplexTypeQueryBigQueryFixture fixture, ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
        Fixture.TestSqlLoggerFactory.Clear();
        Fixture.TestSqlLoggerFactory.SetTestOutputHelper(testOutputHelper);
    }

    #region BigQuery does not support ARRAY in UNION DISTINCT / SELECT DISTINCT

    [ConditionalTheory(Skip = "BigQuery does not support ARRAY type in UNION DISTINCT / SELECT DISTINCT")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Union_complex_type(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support ARRAY type in UNION DISTINCT / SELECT DISTINCT")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Union_entity_type_containing_complex_property(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support ARRAY type in UNION DISTINCT / SELECT DISTINCT")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Union_of_same_entity_with_nested_complex_type_projected_twice_with_pushdown(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support ARRAY type in UNION DISTINCT / SELECT DISTINCT")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Union_of_same_entity_with_nested_complex_type_projected_twice_with_double_pushdown(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support ARRAY type in UNION DISTINCT / SELECT DISTINCT")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Union_of_same_nested_complex_type_projected_twice_with_pushdown(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support ARRAY type in UNION DISTINCT / SELECT DISTINCT")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Union_of_same_nested_complex_type_projected_twice_with_double_pushdown(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support ARRAY type in UNION DISTINCT / SELECT DISTINCT")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Select_complex_type_Distinct(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support ARRAY type in UNION DISTINCT / SELECT DISTINCT")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Load_complex_type_after_subquery_on_entity_type(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support ARRAY type in UNION DISTINCT / SELECT DISTINCT")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Filter_on_property_inside_complex_type_after_subquery(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support ARRAY type in UNION DISTINCT / SELECT DISTINCT")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Filter_on_property_inside_nested_complex_type_after_subquery(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support ARRAY type in UNION DISTINCT / SELECT DISTINCT")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Project_same_nested_complex_type_twice_with_pushdown(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support ARRAY type in UNION DISTINCT / SELECT DISTINCT")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Project_same_nested_complex_type_twice_with_double_pushdown(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support ARRAY type in UNION DISTINCT / SELECT DISTINCT")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Project_same_entity_with_nested_complex_type_twice_with_pushdown(bool async)
        => Task.CompletedTask;

    [ConditionalTheory(Skip = "BigQuery does not support ARRAY type in UNION DISTINCT / SELECT DISTINCT")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Project_same_entity_with_nested_complex_type_twice_with_double_pushdown(bool async)
        => Task.CompletedTask;

    #endregion

    private void AssertSql(params string[] expected)
        => Fixture.TestSqlLoggerFactory.AssertBaseline(expected);

    public class ComplexTypeQueryBigQueryFixture : ComplexTypeQueryRelationalFixtureBase
    {
        protected override ITestStoreFactory TestStoreFactory
            => BigQueryTestStoreFactory.Instance;
    }
}