using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestModels.Northwind;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit.Abstractions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query;

public class NorthwindDbFunctionsQueryBigQueryTest : NorthwindDbFunctionsQueryRelationalTestBase<NorthwindQueryBigQueryFixture<NoopModelCustomizer>>
{
    public NorthwindDbFunctionsQueryBigQueryTest(
        NorthwindQueryBigQueryFixture<NoopModelCustomizer> fixture,
        ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
        Fixture.TestSqlLoggerFactory.Clear();
        Fixture.TestSqlLoggerFactory.SetTestOutputHelper(testOutputHelper);
    }

    #region Like

    public override async Task Like_literal(bool async)
    {
        // BigQuery LIKE is case-sensitive
        await AssertCount(
            async,
            ss => ss.Set<Customer>(),
            ss => ss.Set<Customer>(),
            c => EF.Functions.Like(c.ContactName, "%M%"),
            c => c.ContactName.Contains("M"));

        AssertSql(
            """
SELECT CAST(COUNT(*) AS INT64)
FROM `Customers` AS `c`
WHERE `c`.`ContactName` LIKE '%M%'
""");
    }

    public override async Task Like_identity(bool async)
    {
        await base.Like_identity(async);

        AssertSql(
            """
SELECT CAST(COUNT(*) AS INT64)
FROM `Customers` AS `c`
WHERE `c`.`ContactName` LIKE `c`.`ContactName`
""");
    }

    // BigQuery does not support the ESCAPE clause for LIKE expressions
    [ConditionalTheory(Skip = "BigQuery does not support ESCAPE clause in LIKE expressions")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Like_literal_with_escape(bool async)
        => base.Like_literal_with_escape(async);

    [ConditionalTheory(Skip = "BigQuery does not support ESCAPE clause in LIKE expressions")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Like_all_literals(bool async)
        => base.Like_all_literals(async);

    [ConditionalTheory(Skip = "BigQuery does not support ESCAPE clause in LIKE expressions")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Like_all_literals_with_escape(bool async)
        => base.Like_all_literals_with_escape(async);

    #endregion

    #region Collation

    public override async Task Collate_case_insensitive(bool async)
    {
        await base.Collate_case_insensitive(async);

        AssertSql(
            """
SELECT CAST(COUNT(*) AS INT64)
FROM `Customers` AS `c`
WHERE (COLLATE(`c`.`ContactName`, 'und:ci')) = 'maria anders'
""");
    }

    // BigQuery only supports 'und:ci' collation - case-sensitive is the default behavior
    // and there is no explicit case-sensitive collation name.
    // We provide equivalent tests using 'und:ci' below.
    [ConditionalTheory(Skip = "BigQuery only supports 'und:ci' collation, case-sensitive is the default. See BigQuery-specific tests below.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Collate_case_sensitive(bool async)
        => base.Collate_case_sensitive(async);

    [ConditionalTheory(Skip = "BigQuery only supports 'und:ci' collation, case-sensitive is the default. See BigQuery-specific tests below.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Collate_case_sensitive_constant(bool async)
        => base.Collate_case_sensitive_constant(async);

    public override async Task Collate_is_null(bool async)
    {
        // EF Core simplifies COLLATE away for IS NULL comparisons since collation doesn't affect null checks
        await base.Collate_is_null(async);

        AssertSql(
            """
SELECT CAST(COUNT(*) AS INT64)
FROM `Customers` AS `c`
WHERE `c`.`Region` IS NULL
""");
    }

    // BigQuery collation: only 'und:ci' (case-insensitive) is supported
    // Case-sensitive is the default behavior, no explicit collation name exists
    protected override string CaseInsensitiveCollation => "und:ci";
    protected override string CaseSensitiveCollation => "und:ci"; // Placeholder - tests using this are skipped

    #region BigQuery-specific collation tests (equivalent coverage for skipped tests)

    [Fact]
    public void Collate_case_insensitive_constant()
    {
        // Equivalent to Collate_case_sensitive_constant but using und:ci
        // Tests that COLLATE works on constant values
        using var context = CreateContext();

        var count = context.Set<Customer>()
            .Count(c => c.ContactName == EF.Functions.Collate("MARIA ANDERS", CaseInsensitiveCollation));

        // "Maria Anders" should match "MARIA ANDERS" with case-insensitive collation
        Assert.Equal(1, count);

        AssertSql(
            """
SELECT CAST(COUNT(*) AS INT64)
FROM `Customers` AS `c`
WHERE `c`.`ContactName` = (COLLATE('MARIA ANDERS', 'und:ci'))
""");
    }

    [Fact]
    public void Collate_case_insensitive_column_both_sides()
    {
        // Tests COLLATE applied to column in a comparison
        using var context = CreateContext();

        var count = context.Set<Customer>()
            .Count(c => EF.Functions.Collate(c.ContactName, CaseInsensitiveCollation) == "MARIA ANDERS");

        Assert.Equal(1, count);

        AssertSql(
            """
SELECT CAST(COUNT(*) AS INT64)
FROM `Customers` AS `c`
WHERE (COLLATE(`c`.`ContactName`, 'und:ci')) = 'MARIA ANDERS'
""");
    }

    [Fact]
    public void Collate_case_insensitive_not_equal()
    {
        // Tests COLLATE with not-equal comparison
        using var context = CreateContext();

        var count = context.Set<Customer>()
            .Count(c => EF.Functions.Collate(c.ContactName, CaseInsensitiveCollation) != "MARIA ANDERS");

        // All customers except "Maria Anders"
        Assert.Equal(90, count);

        // EF Core adds "OR column IS NULL" for nullable columns with != comparisons
        AssertSql(
            """
SELECT CAST(COUNT(*) AS INT64)
FROM `Customers` AS `c`
WHERE (COLLATE(`c`.`ContactName`, 'und:ci')) <> 'MARIA ANDERS' OR `c`.`ContactName` IS NULL
""");
    }

    #endregion

    #endregion

    #region Least / Greatest

    public override async Task Least(bool async)
    {
        await base.Least(async);

        AssertSql(
            """
SELECT `o`.`OrderID`, `o`.`ProductID`, `o`.`Discount`, `o`.`Quantity`, `o`.`UnitPrice`
FROM `Order Details` AS `o`
WHERE LEAST(`o`.`OrderID`, 10251) = 10251
""");
    }

    public override async Task Greatest(bool async)
    {
        await base.Greatest(async);

        AssertSql(
            """
SELECT `o`.`OrderID`, `o`.`ProductID`, `o`.`Discount`, `o`.`Quantity`, `o`.`UnitPrice`
FROM `Order Details` AS `o`
WHERE GREATEST(`o`.`OrderID`, 10251) = 10251
""");
    }

    public override async Task Least_with_nullable_value_type(bool async)
    {
        await base.Least_with_nullable_value_type(async);

        AssertSql(
            """
SELECT `o`.`OrderID`, `o`.`ProductID`, `o`.`Discount`, `o`.`Quantity`, `o`.`UnitPrice`
FROM `Order Details` AS `o`
WHERE LEAST(`o`.`OrderID`, 10251) = 10251
""");
    }

    public override async Task Greatest_with_nullable_value_type(bool async)
    {
        await base.Greatest_with_nullable_value_type(async);

        AssertSql(
            """
SELECT `o`.`OrderID`, `o`.`ProductID`, `o`.`Discount`, `o`.`Quantity`, `o`.`UnitPrice`
FROM `Order Details` AS `o`
WHERE GREATEST(`o`.`OrderID`, 10251) = 10251
""");
    }

    #endregion

    #region Random

    public override async Task Random_return_less_than_1(bool async)
    {
        await base.Random_return_less_than_1(async);

        AssertSql(
            """
SELECT CAST(COUNT(*) AS INT64)
FROM `Orders` AS `o`
WHERE RAND() < 1
""");
    }

    public override async Task Random_return_greater_than_0(bool async)
    {
        await base.Random_return_greater_than_0(async);

        AssertSql(
            """
SELECT CAST(COUNT(*) AS INT64)
FROM `Orders` AS `o`
WHERE RAND() >= 0
""");
    }

    #endregion

    private void AssertSql(params string[] expected)
        => Fixture.TestSqlLoggerFactory.AssertBaseline(expected);
}
