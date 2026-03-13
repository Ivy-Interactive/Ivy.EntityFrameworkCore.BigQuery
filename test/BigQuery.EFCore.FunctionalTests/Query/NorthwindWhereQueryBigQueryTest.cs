using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit.Abstractions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query;

public class NorthwindWhereQueryBigQueryTest : NorthwindWhereQueryRelationalTestBase<NorthwindQueryBigQueryFixture<NoopModelCustomizer>>
{
    public NorthwindWhereQueryBigQueryTest(NorthwindQueryBigQueryFixture<NoopModelCustomizer> fixture, ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
        Fixture.TestSqlLoggerFactory.Clear();
        Fixture.TestSqlLoggerFactory.SetTestOutputHelper(testOutputHelper);
    }

    public override async Task Where_ternary_boolean_condition_negated(bool async)
    {
        await base.Where_ternary_boolean_condition_negated(async);

        AssertSql(
            """
SELECT `p`.`ProductID`, `p`.`Discontinued`, `p`.`ProductName`, `p`.`SupplierID`, `p`.`UnitPrice`, `p`.`UnitsInStock`
FROM `Products` AS `p`
WHERE CASE
    WHEN `p`.`UnitsInStock` >= 20 THEN TRUE
    ELSE FALSE
END
""");
    }

    private void AssertSql(params string[] expected)
        => Fixture.TestSqlLoggerFactory.AssertBaseline(expected);

    #region Unsupported: Correlated subqueries with OFFSET

    [ConditionalTheory(Skip = "BigQuery does not support correlated subqueries with OFFSET")]
    [MemberData(nameof(IsAsyncData))]
    public override Task ElementAtOrDefault_over_custom_projection_compared_to_null(bool async)
        => base.ElementAtOrDefault_over_custom_projection_compared_to_null(async);

    [ConditionalTheory(Skip = "BigQuery does not support correlated subqueries with OFFSET")]
    [MemberData(nameof(IsAsyncData))]
    public override Task ElementAt_over_custom_projection_compared_to_not_null(bool async)
        => base.ElementAt_over_custom_projection_compared_to_not_null(async);

    #endregion

    #region Unsupported: Deeply nested correlated subqueries

    [ConditionalTheory(Skip = "BigQuery does not support deeply nested correlated subqueries (EXISTS with nested IN)")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_contains_on_navigation(bool async)
        => base.Where_contains_on_navigation(async);

    #endregion

    #region Unsupported: Anonymous type and tuple comparisons (EF Core issue #14672)

    public override async Task Where_compare_constructed_equal(bool async)
    {
        // Anonymous type to constant comparison. Issue #14672.
        await AssertTranslationFailed(() => base.Where_compare_constructed_equal(async));
    }

    public override async Task Where_compare_constructed_multi_value_equal(bool async)
    {
        // Anonymous type to constant comparison. Issue #14672.
        await AssertTranslationFailed(() => base.Where_compare_constructed_multi_value_equal(async));
    }

    public override async Task Where_compare_constructed_multi_value_not_equal(bool async)
    {
        // Anonymous type to constant comparison. Issue #14672.
        await AssertTranslationFailed(() => base.Where_compare_constructed_multi_value_not_equal(async));
    }

    public override async Task Where_compare_tuple_constructed_equal(bool async)
    {
        // Tuple to constant comparison. Issue #14672.
        await AssertTranslationFailed(() => base.Where_compare_tuple_constructed_equal(async));
    }

    public override async Task Where_compare_tuple_constructed_multi_value_equal(bool async)
    {
        // Tuple to constant comparison. Issue #14672.
        await AssertTranslationFailed(() => base.Where_compare_tuple_constructed_multi_value_equal(async));
    }

    public override async Task Where_compare_tuple_constructed_multi_value_not_equal(bool async)
    {
        // Tuple to constant comparison. Issue #14672.
        await AssertTranslationFailed(() => base.Where_compare_tuple_constructed_multi_value_not_equal(async));
    }

    public override async Task Where_compare_tuple_create_constructed_equal(bool async)
    {
        // Tuple.Create to constant comparison. Issue #14672.
        await AssertTranslationFailed(() => base.Where_compare_tuple_create_constructed_equal(async));
    }

    public override async Task Where_compare_tuple_create_constructed_multi_value_equal(bool async)
    {
        // Tuple.Create to constant comparison. Issue #14672.
        await AssertTranslationFailed(() => base.Where_compare_tuple_create_constructed_multi_value_equal(async));
    }

    public override async Task Where_compare_tuple_create_constructed_multi_value_not_equal(bool async)
    {
        // Tuple.Create to constant comparison. Issue #14672.
        await AssertTranslationFailed(() => base.Where_compare_tuple_create_constructed_multi_value_not_equal(async));
    }

    #endregion
}
