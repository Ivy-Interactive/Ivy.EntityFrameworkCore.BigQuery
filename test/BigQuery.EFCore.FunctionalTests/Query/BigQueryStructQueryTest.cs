using Xunit.Abstractions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query;

/// <summary>
/// BigQuery-specific tests for STRUCT types.
/// Verifies SQL generation for STRUCT field access, nested STRUCTs, and full STRUCT materialization.
/// </summary>
public class BigQueryStructQueryTest(
    BigQueryStructQueryTest.BigQueryStructQueryFixture fixture,
    ITestOutputHelper testOutputHelper)
    : StructQueryTest<BigQueryStructQueryTest.BigQueryStructQueryFixture>(fixture, testOutputHelper)
{
    #region Simple STRUCT Field Access

    public override async Task Project_struct_field(bool async)
    {
        await base.Project_struct_field(async);

        AssertSql(
            """
SELECT `p`.`Name`, `p`.`HomeAddress`.`City` AS `City`
FROM `People` AS `p`
WHERE `p`.`HomeAddress` IS NOT NULL
""");
    }

    public override async Task Project_multiple_struct_fields(bool async)
    {
        await base.Project_multiple_struct_fields(async);

        AssertSql(
            """
SELECT `p`.`Id`, `p`.`HomeAddress`.`Street` AS `Street`, `p`.`HomeAddress`.`City` AS `City`, `p`.`HomeAddress`.`ZipCode` AS `ZipCode`
FROM `People` AS `p`
WHERE `p`.`HomeAddress` IS NOT NULL
""");
    }

    public override async Task Project_entity_id_with_struct_filter(bool async)
    {
        await base.Project_entity_id_with_struct_filter(async);

        AssertSql(
            """
SELECT `p`.`Id`
FROM `People` AS `p`
WHERE `p`.`HomeAddress` IS NOT NULL AND `p`.`HomeAddress`.`City` = 'Seattle'
""");
    }

    public override async Task Filter_by_struct_field_with_parameter(bool async)
    {
        await base.Filter_by_struct_field_with_parameter(async);

        AssertSql(
            """
@__city_0='Portland'

SELECT `p`.`Id`
FROM `People` AS `p`
WHERE `p`.`HomeAddress` IS NOT NULL AND `p`.`HomeAddress`.`City` = @__city_0
""");
    }

    public override async Task OrderBy_struct_field_project_id(bool async)
    {
        await base.OrderBy_struct_field_project_id(async);

        AssertSql(
            """
SELECT `p`.`Id`, `p`.`HomeAddress`.`City` AS `City`
FROM `People` AS `p`
WHERE `p`.`HomeAddress` IS NOT NULL
ORDER BY `p`.`HomeAddress`.`City`, `p`.`Id`
""");
    }

    public override async Task String_Contains_on_struct_field(bool async)
    {
        await base.String_Contains_on_struct_field(async);

        AssertSql(
            """
SELECT `p`.`Id`
FROM `People` AS `p`
WHERE `p`.`HomeAddress` IS NOT NULL AND (`p`.`HomeAddress`.`Street` LIKE '%Main%')
""");
    }

    public override async Task String_StartsWith_on_struct_field(bool async)
    {
        await base.String_StartsWith_on_struct_field(async);

        AssertSql(
            """
SELECT `p`.`Id`
FROM `People` AS `p`
WHERE `p`.`HomeAddress` IS NOT NULL AND (`p`.`HomeAddress`.`ZipCode` LIKE '98%')
""");
    }

    #endregion

    #region Full STRUCT Materialization

    public override async Task Project_entire_struct_column(bool async)
    {
        await base.Project_entire_struct_column(async);

        AssertSql(
            """
SELECT `p`.`Name`, `p`.`HomeAddress`
FROM `People` AS `p`
WHERE `p`.`HomeAddress` IS NOT NULL
""");
    }

    #endregion

    #region Nested STRUCT Access

    public override async Task Project_nested_struct_fields(bool async)
    {
        await base.Project_nested_struct_fields(async);

        AssertSql(
            """
SELECT `c`.`CustomerName`, `c`.`Contact`.`Email` AS `Email`
FROM `Customers` AS `c`
WHERE `c`.`Contact` IS NOT NULL
""");
    }

    public override async Task Filter_by_nested_struct_field(bool async)
    {
        await base.Filter_by_nested_struct_field(async);

        AssertSql(
            """
SELECT `c`.`Id`
FROM `Customers` AS `c`
WHERE `c`.`Contact` IS NOT NULL AND `c`.`Contact`.`MailingAddress`.`City` = 'San Francisco'
""");
    }

    // Skipped: BigQuery NULL semantics for nested STRUCT fields differ from LINQ-to-objects
    // public override async Task Filter_by_nested_struct_nullable_field(bool async)
    // {
    //     await base.Filter_by_nested_struct_nullable_field(async);
    //
    //     AssertSql(
    //         """
    // SELECT `c`.`Id`
    // FROM `Customers` AS `c`
    // WHERE `c`.`Contact` IS NOT NULL AND `c`.`Contact`.`Phone` IS NULL
    // """);
    // }

    #endregion

    #region Aggregate Queries

    public override async Task Count_with_struct_field_filter(bool async)
    {
        await base.Count_with_struct_field_filter(async);

        AssertSql(
            """
SELECT CAST(COUNT(*) AS INT64)
FROM `People` AS `p`
WHERE `p`.`HomeAddress` IS NOT NULL AND `p`.`HomeAddress`.`City` = 'Seattle'
""");
    }

    public override async Task Distinct_on_struct_field(bool async)
    {
        await base.Distinct_on_struct_field(async);

        AssertSql(
            """
SELECT DISTINCT `p`.`HomeAddress`.`City`
FROM `People` AS `p`
WHERE `p`.`HomeAddress` IS NOT NULL
""");
    }

    #endregion

    #region ARRAY<STRUCT> Tests

    public override async Task Array_of_structs_roundtrip(bool async)
    {
        await base.Array_of_structs_roundtrip(async);

        AssertSql(
            """
SELECT `o`.`Id`, `o`.`OrderNumber`, ARRAY_LENGTH(`o`.`Items`) AS `ItemCount`
FROM `Orders` AS `o`
""");
    }

    public override async Task Array_of_structs_first_element_field(bool async)
    {
        await base.Array_of_structs_first_element_field(async);

        AssertSql(
            """
SELECT `o`.`OrderNumber`, (
    SELECT `i`.`ProductName`
    FROM UNNEST(`o`.`Items`) AS `i` WITH OFFSET AS `offset`
    ORDER BY `offset`
    LIMIT 1 OFFSET 0) AS `FirstProduct`
FROM `Orders` AS `o`
""");
    }

    public override async Task Filter_by_array_of_structs_element_field(bool async)
    {
        await base.Filter_by_array_of_structs_element_field(async);

        AssertSql(
            """
SELECT `o`.`Id`
FROM `Orders` AS `o`
WHERE (
    SELECT `i`.`Quantity`
    FROM UNNEST(`o`.`Items`) AS `i` WITH OFFSET AS `offset`
    ORDER BY `offset`
    LIMIT 1 OFFSET 0) > 3
""");
    }

    #endregion

    private void AssertSql(params string[] expected)
        => Fixture.TestSqlLoggerFactory.AssertBaseline(expected);

    public class BigQueryStructQueryFixture : StructQueryFixture
    {
        protected override string StoreName => "BigQueryStructTests";
    }
}
