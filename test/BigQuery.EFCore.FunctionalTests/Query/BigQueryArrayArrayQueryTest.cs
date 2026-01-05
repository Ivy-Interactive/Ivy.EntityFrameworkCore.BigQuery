using Ivy.EFCore.BigQuery.FunctionalTests.TestModels.BigQueryArray;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit;
using Xunit.Abstractions;

namespace Ivy.EFCore.BigQuery.FunctionalTests.Query;

public class BigQueryArrayArrayQueryTest(
    BigQueryArrayArrayQueryTest.BigQueryArrayArrayQueryFixture fixture,
    ITestOutputHelper testOutputHelper)
    : BigQueryArrayQueryTest<BigQueryArrayArrayQueryTest.BigQueryArrayArrayQueryFixture>(fixture, testOutputHelper)
{
    #region Indexers

    public override async Task Index_with_constant(bool async)
    {
        await base.Index_with_constant(async);

        AssertSql(
            """
SELECT `a`.`Id`, `a`.`BoolArray`, `a`.`ByteArray`, `a`.`ContainerId`, `a`.`DoubleArray`, `a`.`DoubleList`, `a`.`IntArray`, `a`.`IntList`, `a`.`LongArray`, `a`.`Name`, `a`.`Score`, `a`.`StringArray`, `a`.`StringList`
FROM `ArrayEntities` AS `a`
WHERE `a`.`IntArray`[OFFSET(0)] = 1
""");
    }

    public override async Task Index_with_parameter(bool async)
    {
        await base.Index_with_parameter(async);

        AssertSql(
            """
@__index_0='0'

SELECT `a`.`Id`, `a`.`BoolArray`, `a`.`ByteArray`, `a`.`ContainerId`, `a`.`DoubleArray`, `a`.`DoubleList`, `a`.`IntArray`, `a`.`IntList`, `a`.`LongArray`, `a`.`Name`, `a`.`Score`, `a`.`StringArray`, `a`.`StringList`
FROM `ArrayEntities` AS `a`
WHERE `a`.`IntArray`[OFFSET(@__index_0)] = 1
""");
    }

    public override async Task String_array_index_with_constant(bool async)
    {
        await base.String_array_index_with_constant(async);

        AssertSql(
            """
SELECT `a`.`Id`, `a`.`BoolArray`, `a`.`ByteArray`, `a`.`ContainerId`, `a`.`DoubleArray`, `a`.`DoubleList`, `a`.`IntArray`, `a`.`IntList`, `a`.`LongArray`, `a`.`Name`, `a`.`Score`, `a`.`StringArray`, `a`.`StringList`
FROM `ArrayEntities` AS `a`
WHERE `a`.`StringArray`[OFFSET(0)] = 'apple'
""");
    }

    public override async Task Index_in_projection(bool async)
    {
        await base.Index_in_projection(async);

        AssertSql(
            """
SELECT `a`.`Id`, `a`.`IntArray`[OFFSET(0)] AS `FirstInt`, `a`.`StringArray`[OFFSET(1)] AS `SecondString`
FROM `ArrayEntities` AS `a`
""");
    }

    #endregion Indexers

    #region ElementAt

    public override async Task ElementAt_with_constant(bool async)
    {
        await base.ElementAt_with_constant(async);

        AssertSql(
            """
SELECT `a`.`Id`, `a`.`BoolArray`, `a`.`ByteArray`, `a`.`ContainerId`, `a`.`DoubleArray`, `a`.`DoubleList`, `a`.`IntArray`, `a`.`IntList`, `a`.`LongArray`, `a`.`Name`, `a`.`Score`, `a`.`StringArray`, `a`.`StringList`
FROM `ArrayEntities` AS `a`
WHERE `a`.`IntArray`[OFFSET(0)] = 1
""");
    }

    public override async Task ElementAt_with_parameter(bool async)
    {
        await base.ElementAt_with_parameter(async);

        AssertSql(
            """
@__index_0='1'

SELECT `a`.`Id`, `a`.`BoolArray`, `a`.`ByteArray`, `a`.`ContainerId`, `a`.`DoubleArray`, `a`.`DoubleList`, `a`.`IntArray`, `a`.`IntList`, `a`.`LongArray`, `a`.`Name`, `a`.`Score`, `a`.`StringArray`, `a`.`StringList`
FROM `ArrayEntities` AS `a`
WHERE `a`.`IntArray`[OFFSET(@__index_0)] = 2
""");
    }

    public override async Task ElementAt_in_projection(bool async)
    {
        await base.ElementAt_in_projection(async);

        AssertSql(
            """
SELECT `a`.`Id`, `a`.`StringArray`[OFFSET(1)] AS `Second`
FROM `ArrayEntities` AS `a`
""");
    }

    #endregion ElementAt

    #region Length/Count

    public override async Task Array_Length(bool async)
    {
        await base.Array_Length(async);

        AssertSql(
            """
SELECT `a`.`Id`, `a`.`BoolArray`, `a`.`ByteArray`, `a`.`ContainerId`, `a`.`DoubleArray`, `a`.`DoubleList`, `a`.`IntArray`, `a`.`IntList`, `a`.`LongArray`, `a`.`Name`, `a`.`Score`, `a`.`StringArray`, `a`.`StringList`
FROM `ArrayEntities` AS `a`
WHERE ARRAY_LENGTH(`a`.`IntArray`) = 3
""");
    }

    public override async Task Array_Length_in_projection(bool async)
    {
        await base.Array_Length_in_projection(async);

        AssertSql(
            """
SELECT `a`.`Id`, ARRAY_LENGTH(`a`.`IntArray`) AS `Count`
FROM `ArrayEntities` AS `a`
""");
    }

    public override async Task List_Count(bool async)
    {
        await base.List_Count(async);

        AssertSql(
            """
SELECT `a`.`Id`, `a`.`BoolArray`, `a`.`ByteArray`, `a`.`ContainerId`, `a`.`DoubleArray`, `a`.`DoubleList`, `a`.`IntArray`, `a`.`IntList`, `a`.`LongArray`, `a`.`Name`, `a`.`Score`, `a`.`StringArray`, `a`.`StringList`
FROM `ArrayEntities` AS `a`
WHERE ARRAY_LENGTH(`a`.`IntList`) = 3
""");
    }

    public override async Task Enumerable_Count(bool async)
    {
        await base.Enumerable_Count(async);

        AssertSql(
            """
SELECT `a`.`Id`, `a`.`BoolArray`, `a`.`ByteArray`, `a`.`ContainerId`, `a`.`DoubleArray`, `a`.`DoubleList`, `a`.`IntArray`, `a`.`IntList`, `a`.`LongArray`, `a`.`Name`, `a`.`Score`, `a`.`StringArray`, `a`.`StringList`
FROM `ArrayEntities` AS `a`
WHERE ARRAY_LENGTH(`a`.`IntArray`) = 3
""");
    }

    public override async Task String_array_Length(bool async)
    {
        await base.String_array_Length(async);

        AssertSql(
            """
SELECT `a`.`Id`, `a`.`BoolArray`, `a`.`ByteArray`, `a`.`ContainerId`, `a`.`DoubleArray`, `a`.`DoubleList`, `a`.`IntArray`, `a`.`IntList`, `a`.`LongArray`, `a`.`Name`, `a`.`Score`, `a`.`StringArray`, `a`.`StringList`
FROM `ArrayEntities` AS `a`
WHERE ARRAY_LENGTH(`a`.`StringArray`) >= 3
""");
    }

    #endregion Length/Count

    #region First/FirstOrDefault

    public override async Task Array_First(bool async)
    {
        await base.Array_First(async);

        AssertSql(
            """
SELECT `a`.`Id`, `a`.`BoolArray`, `a`.`ByteArray`, `a`.`ContainerId`, `a`.`DoubleArray`, `a`.`DoubleList`, `a`.`IntArray`, `a`.`IntList`, `a`.`LongArray`, `a`.`Name`, `a`.`Score`, `a`.`StringArray`, `a`.`StringList`
FROM `ArrayEntities` AS `a`
WHERE `a`.`IntArray`[OFFSET(0)] = 1
""");
    }

    public override async Task Array_FirstOrDefault(bool async)
    {
        await base.Array_FirstOrDefault(async);

        AssertSql(
            """
SELECT `a`.`Id`, `a`.`BoolArray`, `a`.`ByteArray`, `a`.`ContainerId`, `a`.`DoubleArray`, `a`.`DoubleList`, `a`.`IntArray`, `a`.`IntList`, `a`.`LongArray`, `a`.`Name`, `a`.`Score`, `a`.`StringArray`, `a`.`StringList`
FROM `ArrayEntities` AS `a`
WHERE `a`.`IntArray`[OFFSET(0)] = 1
""");
    }

    public override async Task Array_First_in_projection(bool async)
    {
        await base.Array_First_in_projection(async);

        AssertSql(
            """
SELECT `a`.`Id`, `a`.`StringArray`[OFFSET(0)] AS `First`
FROM `ArrayEntities` AS `a`
""");
    }

    public override async Task List_First(bool async)
    {
        await base.List_First(async);

        AssertSql(
            """
SELECT `a`.`Id`, `a`.`BoolArray`, `a`.`ByteArray`, `a`.`ContainerId`, `a`.`DoubleArray`, `a`.`DoubleList`, `a`.`IntArray`, `a`.`IntList`, `a`.`LongArray`, `a`.`Name`, `a`.`Score`, `a`.`StringArray`, `a`.`StringList`
FROM `ArrayEntities` AS `a`
WHERE `a`.`IntList`[OFFSET(0)] = 1
""");
    }

    #endregion First/FirstOrDefault

    #region Complex Queries

    public override async Task Multiple_array_operations_in_projection(bool async)
    {
        await base.Multiple_array_operations_in_projection(async);

        AssertSql(
            """
SELECT `a`.`Id`, ARRAY_LENGTH(`a`.`IntArray`) AS `Length`, `a`.`IntArray`[OFFSET(0)] AS `First`, `a`.`IntArray`[OFFSET(1)] AS `Second`, `a`.`StringArray`[OFFSET(0)] AS `StringFirst`
FROM `ArrayEntities` AS `a`
""");
    }

    public override async Task Array_operations_with_predicates(bool async)
    {
        await base.Array_operations_with_predicates(async);

        AssertSql(
            """
SELECT `a`.`Id`, `a`.`BoolArray`, `a`.`ByteArray`, `a`.`ContainerId`, `a`.`DoubleArray`, `a`.`DoubleList`, `a`.`IntArray`, `a`.`IntList`, `a`.`LongArray`, `a`.`Name`, `a`.`Score`, `a`.`StringArray`, `a`.`StringList`
FROM `ArrayEntities` AS `a`
WHERE ARRAY_LENGTH(`a`.`IntArray`) > 2 AND `a`.`IntArray`[OFFSET(0)] < 5
""");
    }

    public override async Task Array_indexing_with_arithmetic(bool async)
    {
        await base.Array_indexing_with_arithmetic(async);

        AssertSql(
            """
@__offset_0='1'

SELECT `a`.`Id`, `a`.`BoolArray`, `a`.`ByteArray`, `a`.`ContainerId`, `a`.`DoubleArray`, `a`.`DoubleList`, `a`.`IntArray`, `a`.`IntList`, `a`.`LongArray`, `a`.`Name`, `a`.`Score`, `a`.`StringArray`, `a`.`StringList`
FROM `ArrayEntities` AS `a`
WHERE `a`.`IntArray`[OFFSET(0 + @__offset_0)] = 2
""");
    }

    public override async Task Different_array_types_in_same_query(bool async)
    {
        await base.Different_array_types_in_same_query(async);

        AssertSql(
            """
SELECT `a`.`Id`, `a`.`BoolArray`, `a`.`ByteArray`, `a`.`ContainerId`, `a`.`DoubleArray`, `a`.`DoubleList`, `a`.`IntArray`, `a`.`IntList`, `a`.`LongArray`, `a`.`Name`, `a`.`Score`, `a`.`StringArray`, `a`.`StringList`
FROM `ArrayEntities` AS `a`
WHERE ARRAY_LENGTH(`a`.`IntArray`) = 3 AND `a`.`StringArray`[OFFSET(0)] = 'apple'
""");
    }

    #endregion Complex Queries

    #region Ordering

    public override async Task OrderBy_array_length(bool async)
    {
        await base.OrderBy_array_length(async);

        AssertSql(
            """
SELECT `a`.`Id`, `a`.`BoolArray`, `a`.`ByteArray`, `a`.`ContainerId`, `a`.`DoubleArray`, `a`.`DoubleList`, `a`.`IntArray`, `a`.`IntList`, `a`.`LongArray`, `a`.`Name`, `a`.`Score`, `a`.`StringArray`, `a`.`StringList`
FROM `ArrayEntities` AS `a`
ORDER BY ARRAY_LENGTH(`a`.`IntArray`), `a`.`Id`
""");
    }

    public override async Task OrderBy_array_first_element(bool async)
    {
        await base.OrderBy_array_first_element(async);

        AssertSql(
            """
SELECT `a`.`Id`, `a`.`BoolArray`, `a`.`ByteArray`, `a`.`ContainerId`, `a`.`DoubleArray`, `a`.`DoubleList`, `a`.`IntArray`, `a`.`IntList`, `a`.`LongArray`, `a`.`Name`, `a`.`Score`, `a`.`StringArray`, `a`.`StringList`
FROM `ArrayEntities` AS `a`
ORDER BY `a`.`IntArray`[OFFSET(0)], `a`.`Id`
""");
    }

    #endregion Ordering

    #region Subqueries

    public override async Task Array_in_scalar_subquery(bool async)
    {
        await base.Array_in_scalar_subquery(async);

        AssertSql(
            """
SELECT `a`.`Id`, `a`.`ArrayEntities`
FROM `ArrayContainers` AS `a`
WHERE (
    SELECT ARRAY_LENGTH(`a0`.`IntArray`)
    FROM `ArrayEntities` AS `a0`
    WHERE `a`.`Id` = `a0`.`ContainerId`
    ORDER BY `a0`.`Id`
    LIMIT 1) > 0
""");
    }

    #endregion Subqueries

    #region Null Comparisons

    public override async Task Non_nullable_value_array_index_compare_to_null(bool async)
    {
        await base.Non_nullable_value_array_index_compare_to_null(async);

        AssertSql(
            """
SELECT `a`.`Id`, `a`.`BoolArray`, `a`.`ByteArray`, `a`.`ContainerId`, `a`.`DoubleArray`, `a`.`DoubleList`, `a`.`IntArray`, `a`.`IntList`, `a`.`LongArray`, `a`.`Name`, `a`.`Score`, `a`.`StringArray`, `a`.`StringList`
FROM `ArrayEntities` AS `a`
WHERE `a`.`IntArray`[OFFSET(1)] IS NULL
""");
    }

    public override async Task Non_nullable_reference_array_index_compare_to_null(bool async)
    {
        await base.Non_nullable_reference_array_index_compare_to_null(async);

        AssertSql(
            """
SELECT `a`.`Id`, `a`.`BoolArray`, `a`.`ByteArray`, `a`.`ContainerId`, `a`.`DoubleArray`, `a`.`DoubleList`, `a`.`IntArray`, `a`.`IntList`, `a`.`LongArray`, `a`.`Name`, `a`.`Score`, `a`.`StringArray`, `a`.`StringList`
FROM `ArrayEntities` AS `a`
WHERE `a`.`StringArray`[OFFSET(1)] IS NULL
""");
    }

    #endregion Null Comparisons

    #region EF.Property

    public override async Task Array_Length_on_EF_Property(bool async)
    {
        await base.Array_Length_on_EF_Property(async);

        AssertSql(
            """
SELECT `a`.`Id`, `a`.`BoolArray`, `a`.`ByteArray`, `a`.`ContainerId`, `a`.`DoubleArray`, `a`.`DoubleList`, `a`.`IntArray`, `a`.`IntList`, `a`.`LongArray`, `a`.`Name`, `a`.`Score`, `a`.`StringArray`, `a`.`StringList`
FROM `ArrayEntities` AS `a`
WHERE ARRAY_LENGTH(`a`.`IntArray`) = 3
""");
    }

    #endregion EF.Property

    #region SelectMany

    public override async Task SelectMany_array_column(bool async)
    {
        await base.SelectMany_array_column(async);

        AssertSql(
            """
SELECT `i`
FROM `ArrayEntities` AS `a`
CROSS JOIN UNNEST(`a`.`IntArray`) AS `i` WITH OFFSET AS `offset`
""");
    }

    public override async Task SelectMany_with_projection(bool async)
    {
        await base.SelectMany_with_projection(async);

        AssertSql(
            """
SELECT `a`.`Id`, `i` AS `Value`
FROM `ArrayEntities` AS `a`
CROSS JOIN UNNEST(`a`.`IntArray`) AS `i` WITH OFFSET AS `offset`
""");
    }

    #endregion SelectMany

    #region SequenceEqual

    public override async Task SequenceEqual_with_parameter(bool async)
    {
        await base.SequenceEqual_with_parameter(async);

        AssertSql(
            """
@__arr_0='[1,2,3]' (DbType = Object)

SELECT `a`.`Id`, `a`.`BoolArray`, `a`.`ByteArray`, `a`.`ContainerId`, `a`.`DoubleArray`, `a`.`DoubleList`, `a`.`IntArray`, `a`.`IntList`, `a`.`LongArray`, `a`.`Name`, `a`.`Score`, `a`.`StringArray`, `a`.`StringList`
FROM `ArrayEntities` AS `a`
WHERE `a`.`IntArray` = @__arr_0
""");
    }

    public override async Task SequenceEqual_with_different_parameter(bool async)
    {
        await base.SequenceEqual_with_different_parameter(async);

        AssertSql(
            """
@__arr_0='[4,5,6,7]' (DbType = Object)

SELECT `a`.`Id`, `a`.`BoolArray`, `a`.`ByteArray`, `a`.`ContainerId`, `a`.`DoubleArray`, `a`.`DoubleList`, `a`.`IntArray`, `a`.`IntList`, `a`.`LongArray`, `a`.`Name`, `a`.`Score`, `a`.`StringArray`, `a`.`StringList`
FROM `ArrayEntities` AS `a`
WHERE `a`.`IntArray` = @__arr_0
""");
    }

    #endregion SequenceEqual

    #region Containment

    public override async Task Array_column_Contains_literal_item(bool async)
    {
        await base.Array_column_Contains_literal_item(async);

        AssertSql(
            """
SELECT `a`.`Id`, `a`.`BoolArray`, `a`.`ByteArray`, `a`.`ContainerId`, `a`.`DoubleArray`, `a`.`DoubleList`, `a`.`IntArray`, `a`.`IntList`, `a`.`LongArray`, `a`.`Name`, `a`.`Score`, `a`.`StringArray`, `a`.`StringList`
FROM `ArrayEntities` AS `a`
WHERE 1 IN (
    SELECT `i`
    FROM UNNEST(`a`.`IntArray`) AS `i` WITH OFFSET AS `offset`
)
""");
    }

    public override async Task Array_column_Contains_parameter_item(bool async)
    {
        await base.Array_column_Contains_parameter_item(async);

        AssertSql(
            """
@__p_0='1'

SELECT `a`.`Id`, `a`.`BoolArray`, `a`.`ByteArray`, `a`.`ContainerId`, `a`.`DoubleArray`, `a`.`DoubleList`, `a`.`IntArray`, `a`.`IntList`, `a`.`LongArray`, `a`.`Name`, `a`.`Score`, `a`.`StringArray`, `a`.`StringList`
FROM `ArrayEntities` AS `a`
WHERE @__p_0 IN (
    SELECT `i`
    FROM UNNEST(`a`.`IntArray`) AS `i` WITH OFFSET AS `offset`
)
""");
    }

    public override async Task Array_column_Contains_column_item(bool async)
    {
        await base.Array_column_Contains_column_item(async);

        AssertSql(
            """
SELECT `a`.`Id`, `a`.`BoolArray`, `a`.`ByteArray`, `a`.`ContainerId`, `a`.`DoubleArray`, `a`.`DoubleList`, `a`.`IntArray`, `a`.`IntList`, `a`.`LongArray`, `a`.`Name`, `a`.`Score`, `a`.`StringArray`, `a`.`StringList`
FROM `ArrayEntities` AS `a`
WHERE `a`.`Id` IN (
    SELECT `i`
    FROM UNNEST(`a`.`IntArray`) AS `i` WITH OFFSET AS `offset`
)
""");
    }

    public override async Task String_array_Contains_literal(bool async)
    {
        await base.String_array_Contains_literal(async);

        AssertSql(
            """
SELECT `a`.`Id`, `a`.`BoolArray`, `a`.`ByteArray`, `a`.`ContainerId`, `a`.`DoubleArray`, `a`.`DoubleList`, `a`.`IntArray`, `a`.`IntList`, `a`.`LongArray`, `a`.`Name`, `a`.`Score`, `a`.`StringArray`, `a`.`StringList`
FROM `ArrayEntities` AS `a`
WHERE 'apple' IN (
    SELECT `s`
    FROM UNNEST(`a`.`StringArray`) AS `s` WITH OFFSET AS `offset`
)
""");
    }

    public override async Task Array_constant_Contains_column(bool async)
    {
        await base.Array_constant_Contains_column(async);

        AssertSql(
            """
SELECT `a`.`Id`, `a`.`BoolArray`, `a`.`ByteArray`, `a`.`ContainerId`, `a`.`DoubleArray`, `a`.`DoubleList`, `a`.`IntArray`, `a`.`IntList`, `a`.`LongArray`, `a`.`Name`, `a`.`Score`, `a`.`StringArray`, `a`.`StringList`
FROM `ArrayEntities` AS `a`
WHERE `a`.`Id` IN (1, 2)
""");
    }

    public override async Task Array_param_Contains_column(bool async)
    {
        await base.Array_param_Contains_column(async);

        AssertSql(
            """
SELECT `a`.`Id`, `a`.`BoolArray`, `a`.`ByteArray`, `a`.`ContainerId`, `a`.`DoubleArray`, `a`.`DoubleList`, `a`.`IntArray`, `a`.`IntList`, `a`.`LongArray`, `a`.`Name`, `a`.`Score`, `a`.`StringArray`, `a`.`StringList`
FROM `ArrayEntities` AS `a`
WHERE `a`.`Id` IN (1, 2)
""");
    }

    #endregion Containment

    #region Any/All

    public override async Task Any_no_predicate(bool async)
    {
        await base.Any_no_predicate(async);

        AssertSql(
            """
SELECT `a`.`Id`, `a`.`BoolArray`, `a`.`ByteArray`, `a`.`ContainerId`, `a`.`DoubleArray`, `a`.`DoubleList`, `a`.`IntArray`, `a`.`IntList`, `a`.`LongArray`, `a`.`Name`, `a`.`Score`, `a`.`StringArray`, `a`.`StringList`
FROM `ArrayEntities` AS `a`
WHERE EXISTS (
    SELECT 1
    FROM UNNEST(`a`.`IntArray`) AS `i` WITH OFFSET AS `offset`)
""");
    }

    public override async Task Any_with_predicate(bool async)
    {
        await base.Any_with_predicate(async);

        AssertSql(
            """
SELECT `a`.`Id`, `a`.`BoolArray`, `a`.`ByteArray`, `a`.`ContainerId`, `a`.`DoubleArray`, `a`.`DoubleList`, `a`.`IntArray`, `a`.`IntList`, `a`.`LongArray`, `a`.`Name`, `a`.`Score`, `a`.`StringArray`, `a`.`StringList`
FROM `ArrayEntities` AS `a`
WHERE EXISTS (
    SELECT 1
    FROM UNNEST(`a`.`IntArray`) AS `i` WITH OFFSET AS `offset`
    WHERE `i` > 5)
""");
    }

    public override async Task All_with_predicate(bool async)
    {
        await base.All_with_predicate(async);

        AssertSql(
            """
SELECT `a`.`Id`, `a`.`BoolArray`, `a`.`ByteArray`, `a`.`ContainerId`, `a`.`DoubleArray`, `a`.`DoubleList`, `a`.`IntArray`, `a`.`IntList`, `a`.`LongArray`, `a`.`Name`, `a`.`Score`, `a`.`StringArray`, `a`.`StringList`
FROM `ArrayEntities` AS `a`
WHERE NOT EXISTS (
    SELECT 1
    FROM UNNEST(`a`.`IntArray`) AS `i` WITH OFFSET AS `offset`
    WHERE `i` <= 0)
""");
    }

    [ConditionalTheory(Skip = "BigQuery does not support VALUES syntax in UNION ALL - needs SELECT instead")]
    [MemberData(nameof(IsAsyncData))]
    public override async Task Any_Contains_on_constant_array(bool async)
    {
        await base.Any_Contains_on_constant_array(async);
    }

    [ConditionalTheory(Skip = "BigQuery does not support VALUES syntax in UNION ALL - needs SELECT instead")]
    [MemberData(nameof(IsAsyncData))]
    public override async Task All_Contains(bool async)
    {
        await base.All_Contains(async);
    }

    #endregion Any/All

    #region Concat/Reverse

    [ConditionalTheory(Skip = "EF Core limitation: Collection projections with ToArray() require ARRAY() subquery translation")]
    [MemberData(nameof(IsAsyncData))]
    public override async Task Array_Concat_with_constant(bool async)
    {
        await base.Array_Concat_with_constant(async);
    }

    [ConditionalTheory(Skip = "EF Core limitation: Collection projections with ToArray() require ARRAY() subquery translation")]
    [MemberData(nameof(IsAsyncData))]
    public override async Task Array_Concat_two_columns(bool async)
    {
        await base.Array_Concat_two_columns(async);
    }

    [ConditionalTheory(Skip = "EF Core limitation: Collection projections with ToArray() require ARRAY() subquery translation")]
    [MemberData(nameof(IsAsyncData))]
    public override async Task Array_Reverse(bool async)
    {
        await base.Array_Reverse(async);
    }

    [ConditionalTheory(Skip = "EF Core limitation: Collection projections with ToArray() require ARRAY() subquery translation")]
    [MemberData(nameof(IsAsyncData))]
    public override async Task Array_Concat_then_Reverse(bool async)
    {
        await base.Array_Concat_then_Reverse(async);
    }

    #endregion Concat/Reverse

    private void AssertSql(params string[] expected)
        => Fixture.TestSqlLoggerFactory.AssertBaseline(expected);

    public class BigQueryArrayArrayQueryFixture : BigQueryArrayQueryFixture
    {
        protected override string StoreName => "BigQueryArrayTests";
    }
}
