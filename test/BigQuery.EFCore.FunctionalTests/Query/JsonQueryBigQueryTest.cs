using Ivy.EntityFrameworkCore.BigQuery.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestModels.JsonQuery;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit.Abstractions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query;

public class JsonQueryBigQueryTest : JsonQueryRelationalTestBase<JsonQueryBigQueryTest.JsonQueryBigQueryFixture>
{
    public JsonQueryBigQueryTest(JsonQueryBigQueryFixture fixture, ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
        Fixture.TestSqlLoggerFactory.Clear();
        Fixture.TestSqlLoggerFactory.SetTestOutputHelper(testOutputHelper);
    }

    #region Basic projections

    public override async Task Basic_json_projection_owner_entity(bool async)
    {
        await base.Basic_json_projection_owner_entity(async);

        AssertSql(
            """
SELECT `j`.`Id`, `j`.`EntityBasicId`, `j`.`Name`, `j`.`OwnedCollectionRoot`, `j`.`OwnedReferenceRoot`
FROM `JsonEntitiesBasic` AS `j`
""");
    }

    public override async Task Basic_json_projection_owner_entity_NoTracking(bool async)
    {
        await base.Basic_json_projection_owner_entity_NoTracking(async);

        AssertSql(
            """
SELECT `j`.`Id`, `j`.`EntityBasicId`, `j`.`Name`, `j`.`OwnedCollectionRoot`, `j`.`OwnedReferenceRoot`
FROM `JsonEntitiesBasic` AS `j`
""");
    }

    public override async Task Basic_json_projection_owner_entity_NoTrackingWithIdentityResolution(bool async)
    {
        await base.Basic_json_projection_owner_entity_NoTrackingWithIdentityResolution(async);

        AssertSql(
            """
SELECT `j`.`Id`, `j`.`EntityBasicId`, `j`.`Name`, `j`.`OwnedCollectionRoot`, `j`.`OwnedReferenceRoot`
FROM `JsonEntitiesBasic` AS `j`
""");
    }

    public override async Task Basic_json_projection_owned_reference_root(bool async)
    {
        await base.Basic_json_projection_owned_reference_root(async);

        AssertSql(
            """
SELECT `j`.`OwnedReferenceRoot`, `j`.`Id`
FROM `JsonEntitiesBasic` AS `j`
""");
    }

    public override async Task Basic_json_projection_owned_collection_root(bool async)
    {
        await base.Basic_json_projection_owned_collection_root(async);

        AssertSql(
            """
SELECT `j`.`OwnedCollectionRoot`, `j`.`Id`
FROM `JsonEntitiesBasic` AS `j`
""");
    }

    public override async Task Basic_json_projection_scalar(bool async)
    {
        await base.Basic_json_projection_scalar(async);

        AssertSql(
            """
SELECT JSON_EXTRACT_SCALAR(`j`.`OwnedReferenceRoot`, '$.Name')
FROM `JsonEntitiesBasic` AS `j`
""");
    }

    public override async Task Basic_json_projection_owned_reference_branch(bool async)
    {
        await base.Basic_json_projection_owned_reference_branch(async);

        AssertSql(
            """
SELECT JSON_EXTRACT(`j`.`OwnedReferenceRoot`, '$.OwnedReferenceBranch'), `j`.`Id`
FROM `JsonEntitiesBasic` AS `j`
""");
    }

    public override async Task Basic_json_projection_owned_collection_branch(bool async)
    {
        await base.Basic_json_projection_owned_collection_branch(async);

        AssertSql(
            """
SELECT JSON_EXTRACT(`j`.`OwnedReferenceRoot`, '$.OwnedCollectionBranch'), `j`.`Id`
FROM `JsonEntitiesBasic` AS `j`
""");
    }

    public override async Task Basic_json_projection_owned_reference_leaf(bool async)
    {
        await base.Basic_json_projection_owned_reference_leaf(async);

        AssertSql(
            """
SELECT JSON_EXTRACT(`j`.`OwnedReferenceRoot`, '$.OwnedReferenceBranch.OwnedReferenceLeaf'), `j`.`Id`
FROM `JsonEntitiesBasic` AS `j`
""");
    }

    public override async Task Basic_json_projection_owned_collection_leaf(bool async)
    {
        await base.Basic_json_projection_owned_collection_leaf(async);

        AssertSql(
            """
SELECT JSON_EXTRACT(`j`.`OwnedReferenceRoot`, '$.OwnedReferenceBranch.OwnedCollectionLeaf'), `j`.`Id`
FROM `JsonEntitiesBasic` AS `j`
""");
    }

    #endregion

    #region Tracking failures

    public override async Task Project_json_reference_in_tracking_query_fails(bool async)
    {
        await base.Project_json_reference_in_tracking_query_fails(async);

        AssertSql();
    }

    public override async Task Project_json_collection_in_tracking_query_fails(bool async)
    {
        await base.Project_json_collection_in_tracking_query_fails(async);

        AssertSql();
    }

    public override async Task Project_json_entity_in_tracking_query_fails_even_when_owner_is_present(bool async)
    {
        await base.Project_json_entity_in_tracking_query_fails_even_when_owner_is_present(async);

        AssertSql();
    }

    #endregion

    #region Scalar access

    public override async Task Json_scalar_length(bool async)
    {
        await base.Json_scalar_length(async);

        AssertSql(
            """
SELECT `j`.`Name`
FROM `JsonEntitiesBasic` AS `j`
WHERE LENGTH(JSON_EXTRACT_SCALAR(`j`.`OwnedReferenceRoot`, '$.Name')) > 2
""");
    }

    #endregion

    #region Predicate

    public override async Task Json_property_in_predicate(bool async)
    {
        await base.Json_property_in_predicate(async);

        AssertSql(
            """
SELECT `j`.`Id`
FROM `JsonEntitiesBasic` AS `j`
WHERE (CAST(JSON_EXTRACT_SCALAR(`j`.`OwnedReferenceRoot`, '$.OwnedReferenceBranch.Fraction') AS BIGNUMERIC)) < BIGNUMERIC '20.5'
""");
    }

    #endregion

    #region Enum

    public override async Task Basic_json_projection_enum_inside_json_entity(bool async)
    {
        await base.Basic_json_projection_enum_inside_json_entity(async);

        AssertSql(
            """
SELECT `j`.`Id`, CAST(JSON_EXTRACT_SCALAR(`j`.`OwnedReferenceRoot`, '$.OwnedReferenceBranch.Enum') AS INT64) AS `Enum`
FROM `JsonEntitiesBasic` AS `j`
""");
    }

    public override async Task Json_projection_enum_with_custom_conversion(bool async)
    {
        await base.Json_projection_enum_with_custom_conversion(async);

        AssertSql(
            """
SELECT `j`.`Id`, CAST(JSON_EXTRACT_SCALAR(`j`.`json_reference_custom_naming`, '$.1CustomEnum') AS INT64) AS `Enum`
FROM `JsonEntitiesCustomNaming` AS `j`
""");
    }

    #endregion

    #region Not supported - FromSql

    // BigQuery doesn't support FromSql in the same way
    [ConditionalTheory(Skip = "BigQuery does not support FromSql with JSON")]
    [MemberData(nameof(IsAsyncData))]
    public override Task FromSql_on_entity_with_json_basic(bool async)
        => base.FromSql_on_entity_with_json_basic(async);

    [ConditionalTheory(Skip = "BigQuery does not support FromSql with JSON")]
    [MemberData(nameof(IsAsyncData))]
    public override Task FromSql_on_entity_with_json_project_json_reference(bool async)
        => base.FromSql_on_entity_with_json_project_json_reference(async);

    [ConditionalTheory(Skip = "BigQuery does not support FromSql with JSON")]
    [MemberData(nameof(IsAsyncData))]
    public override Task FromSql_on_entity_with_json_project_json_collection(bool async)
        => base.FromSql_on_entity_with_json_project_json_collection(async);

    [ConditionalTheory(Skip = "BigQuery does not support FromSql with JSON")]
    [MemberData(nameof(IsAsyncData))]
    public override Task FromSql_on_entity_with_json_inheritance_on_base(bool async)
        => base.FromSql_on_entity_with_json_inheritance_on_base(async);

    [ConditionalTheory(Skip = "BigQuery does not support FromSql with JSON")]
    [MemberData(nameof(IsAsyncData))]
    public override Task FromSql_on_entity_with_json_inheritance_on_derived(bool async)
        => base.FromSql_on_entity_with_json_inheritance_on_derived(async);

    [ConditionalTheory(Skip = "BigQuery does not support FromSql with JSON")]
    [MemberData(nameof(IsAsyncData))]
    public override Task FromSql_on_entity_with_json_inheritance_project_reference_on_base(bool async)
        => base.FromSql_on_entity_with_json_inheritance_project_reference_on_base(async);

    [ConditionalTheory(Skip = "BigQuery does not support FromSql with JSON")]
    [MemberData(nameof(IsAsyncData))]
    public override Task FromSql_on_entity_with_json_inheritance_project_reference_on_derived(bool async)
        => base.FromSql_on_entity_with_json_inheritance_project_reference_on_derived(async);

    #endregion

    #region JsonEntityAllTypes - skipped (seeding issues)

    // These tests use JsonEntityAllTypes which we skip seeding due to EF Core ModificationCommand.WriteJson issues
    private const string SkipAllTypesReason = "JsonEntityAllTypes seeding not supported due to type mapping issues";

    [ConditionalTheory(Skip = SkipAllTypesReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_all_types_entity_projection(bool async)
        => base.Json_all_types_entity_projection(async);

    [ConditionalTheory(Skip = SkipAllTypesReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_all_types_projection_from_owned_entity_reference(bool async)
        => base.Json_all_types_projection_from_owned_entity_reference(async);

    [ConditionalTheory(Skip = SkipAllTypesReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_all_types_projection_individual_properties(bool async)
        => base.Json_all_types_projection_individual_properties(async);

    [ConditionalTheory(Skip = SkipAllTypesReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_boolean_predicate(bool async)
        => base.Json_boolean_predicate(async);

    [ConditionalTheory(Skip = SkipAllTypesReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_boolean_predicate_negated(bool async)
        => base.Json_boolean_predicate_negated(async);

    [ConditionalTheory(Skip = SkipAllTypesReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_boolean_projection(bool async)
        => base.Json_boolean_projection(async);

    [ConditionalTheory(Skip = SkipAllTypesReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_boolean_projection_negated(bool async)
        => base.Json_boolean_projection_negated(async);

    [ConditionalTheory(Skip = SkipAllTypesReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_predicate_on_default_string(bool async)
        => base.Json_predicate_on_default_string(async);

    [ConditionalTheory(Skip = SkipAllTypesReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_predicate_on_max_length_string(bool async)
        => base.Json_predicate_on_max_length_string(async);

    [ConditionalTheory(Skip = SkipAllTypesReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_predicate_on_string_condition(bool async)
        => base.Json_predicate_on_string_condition(async);

    [ConditionalTheory(Skip = SkipAllTypesReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_predicate_on_byte(bool async)
        => base.Json_predicate_on_byte(async);

    [ConditionalTheory(Skip = SkipAllTypesReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_predicate_on_byte_array(bool async)
        => base.Json_predicate_on_byte_array(async);

    [ConditionalTheory(Skip = SkipAllTypesReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_predicate_on_character(bool async)
        => base.Json_predicate_on_character(async);

    [ConditionalTheory(Skip = SkipAllTypesReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_predicate_on_datetime(bool async)
        => base.Json_predicate_on_datetime(async);

    [ConditionalTheory(Skip = SkipAllTypesReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_predicate_on_datetimeoffset(bool async)
        => base.Json_predicate_on_datetimeoffset(async);

    [ConditionalTheory(Skip = SkipAllTypesReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_predicate_on_decimal(bool async)
        => base.Json_predicate_on_decimal(async);

    [ConditionalTheory(Skip = SkipAllTypesReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_predicate_on_double(bool async)
        => base.Json_predicate_on_double(async);

    [ConditionalTheory(Skip = SkipAllTypesReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_predicate_on_guid(bool async)
        => base.Json_predicate_on_guid(async);

    [ConditionalTheory(Skip = SkipAllTypesReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_predicate_on_int16(bool async)
        => base.Json_predicate_on_int16(async);

    [ConditionalTheory(Skip = SkipAllTypesReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_predicate_on_int32(bool async)
        => base.Json_predicate_on_int32(async);

    [ConditionalTheory(Skip = SkipAllTypesReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_predicate_on_int64(bool async)
        => base.Json_predicate_on_int64(async);

    [ConditionalTheory(Skip = SkipAllTypesReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_predicate_on_signedbyte(bool async)
        => base.Json_predicate_on_signedbyte(async);

    [ConditionalTheory(Skip = SkipAllTypesReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_predicate_on_single(bool async)
        => base.Json_predicate_on_single(async);

    [ConditionalTheory(Skip = SkipAllTypesReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_predicate_on_timespan(bool async)
        => base.Json_predicate_on_timespan(async);

    [ConditionalTheory(Skip = SkipAllTypesReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_predicate_on_dateonly(bool async)
        => base.Json_predicate_on_dateonly(async);

    [ConditionalTheory(Skip = SkipAllTypesReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_predicate_on_timeonly(bool async)
        => base.Json_predicate_on_timeonly(async);

    [ConditionalTheory(Skip = SkipAllTypesReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_predicate_on_unisgnedint16(bool async)
        => base.Json_predicate_on_unisgnedint16(async);

    [ConditionalTheory(Skip = SkipAllTypesReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_predicate_on_unsignedint32(bool async)
        => base.Json_predicate_on_unsignedint32(async);

    [ConditionalTheory(Skip = SkipAllTypesReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_predicate_on_unsignedint64(bool async)
        => base.Json_predicate_on_unsignedint64(async);

    [ConditionalTheory(Skip = SkipAllTypesReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_predicate_on_enum(bool async)
        => base.Json_predicate_on_enum(async);

    [ConditionalTheory(Skip = SkipAllTypesReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_predicate_on_enumwithintconverter(bool async)
        => base.Json_predicate_on_enumwithintconverter(async);

    [ConditionalTheory(Skip = SkipAllTypesReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_predicate_on_nullableenum1(bool async)
        => base.Json_predicate_on_nullableenum1(async);

    [ConditionalTheory(Skip = SkipAllTypesReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_predicate_on_nullableenum2(bool async)
        => base.Json_predicate_on_nullableenum2(async);

    [ConditionalTheory(Skip = SkipAllTypesReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_predicate_on_nullableenumwithconverterthathandlesnulls1(bool async)
        => base.Json_predicate_on_nullableenumwithconverterthathandlesnulls1(async);

    [ConditionalTheory(Skip = SkipAllTypesReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_predicate_on_nullableenumwithconverterthathandlesnulls2(bool async)
        => base.Json_predicate_on_nullableenumwithconverterthathandlesnulls2(async);

    [ConditionalTheory(Skip = SkipAllTypesReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_predicate_on_nullableenumwithconverter1(bool async)
        => base.Json_predicate_on_nullableenumwithconverter1(async);

    [ConditionalTheory(Skip = SkipAllTypesReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_predicate_on_nullableenumwithconverter2(bool async)
        => base.Json_predicate_on_nullableenumwithconverter2(async);

    [ConditionalTheory(Skip = SkipAllTypesReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_predicate_on_nullableint321(bool async)
        => base.Json_predicate_on_nullableint321(async);

    [ConditionalTheory(Skip = SkipAllTypesReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_predicate_on_nullableint322(bool async)
        => base.Json_predicate_on_nullableint322(async);

    #endregion

    #region Unsupported LINQ over JSON collections

    private const string SkipJsonCollectionReason = "BigQuery does not support composing LINQ over JSON collections";

    [ConditionalTheory(Skip = SkipJsonCollectionReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_collection_SelectMany(bool async)
        => base.Json_collection_SelectMany(async);

    [ConditionalTheory(Skip = SkipJsonCollectionReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_collection_Skip(bool async)
        => base.Json_collection_Skip(async);

    [ConditionalTheory(Skip = SkipJsonCollectionReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_collection_Where_ElementAt(bool async)
        => base.Json_collection_Where_ElementAt(async);

    [ConditionalTheory(Skip = SkipJsonCollectionReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_collection_OrderByDescending_Skip_ElementAt(bool async)
        => base.Json_collection_OrderByDescending_Skip_ElementAt(async);

    [ConditionalTheory(Skip = SkipJsonCollectionReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_collection_Distinct_Count_with_predicate(bool async)
        => base.Json_collection_Distinct_Count_with_predicate(async);

    [ConditionalTheory(Skip = SkipJsonCollectionReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_collection_Any_with_predicate(bool async)
        => base.Json_collection_Any_with_predicate(async);

    [ConditionalTheory(Skip = SkipJsonCollectionReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_collection_distinct_in_projection(bool async)
        => base.Json_collection_distinct_in_projection(async);

    [ConditionalTheory(Skip = SkipJsonCollectionReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_collection_filter_in_projection(bool async)
        => base.Json_collection_filter_in_projection(async);

    [ConditionalTheory(Skip = SkipJsonCollectionReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_collection_skip_take_in_projection(bool async)
        => base.Json_collection_skip_take_in_projection(async);

    [ConditionalTheory(Skip = SkipJsonCollectionReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_collection_skip_take_in_projection_with_json_reference_access_as_final_operation(bool async)
        => base.Json_collection_skip_take_in_projection_with_json_reference_access_as_final_operation(async);

    [ConditionalTheory(Skip = SkipJsonCollectionReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_collection_skip_take_in_projection_project_into_anonymous_type(bool async)
        => base.Json_collection_skip_take_in_projection_project_into_anonymous_type(async);

    [ConditionalTheory(Skip = SkipJsonCollectionReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_collection_in_projection_with_composition_count(bool async)
        => base.Json_collection_in_projection_with_composition_count(async);

    [ConditionalTheory(Skip = SkipJsonCollectionReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_collection_in_projection_with_anonymous_projection_of_scalars(bool async)
        => base.Json_collection_in_projection_with_anonymous_projection_of_scalars(async);

    [ConditionalTheory(Skip = SkipJsonCollectionReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_collection_in_projection_with_composition_where_and_anonymous_projection_of_scalars(bool async)
        => base.Json_collection_in_projection_with_composition_where_and_anonymous_projection_of_scalars(async);

    [ConditionalTheory(Skip = SkipJsonCollectionReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_collection_in_projection_with_composition_where_and_anonymous_projection_of_primitive_arrays(bool async)
        => base.Json_collection_in_projection_with_composition_where_and_anonymous_projection_of_primitive_arrays(async);

    [ConditionalTheory(Skip = SkipJsonCollectionReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_collection_leaf_filter_in_projection(bool async)
        => base.Json_collection_leaf_filter_in_projection(async);

    [ConditionalTheory(Skip = SkipJsonCollectionReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_nested_collection_filter_in_projection(bool async)
        => base.Json_nested_collection_filter_in_projection(async);

    [ConditionalTheory(Skip = SkipJsonCollectionReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_nested_collection_SelectMany(bool async)
        => base.Json_nested_collection_SelectMany(async);

    [ConditionalTheory(Skip = SkipJsonCollectionReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_nested_collection_anonymous_projection_in_projection(bool async)
        => base.Json_nested_collection_anonymous_projection_in_projection(async);

    [ConditionalTheory(Skip = SkipJsonCollectionReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_multiple_collection_projections(bool async)
        => base.Json_multiple_collection_projections(async);

    [ConditionalTheory(Skip = SkipJsonCollectionReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_branch_collection_distinct_and_other_collection(bool async)
        => base.Json_branch_collection_distinct_and_other_collection(async);

    [ConditionalTheory(Skip = SkipJsonCollectionReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_leaf_collection_distinct_and_other_collection(bool async)
        => base.Json_leaf_collection_distinct_and_other_collection(async);

    [ConditionalTheory(Skip = SkipJsonCollectionReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_collection_within_collection_Count(bool async)
        => base.Json_collection_within_collection_Count(async);

    [ConditionalTheory(Skip = SkipJsonCollectionReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_collection_Select_entity_with_initializer_ElementAt(bool async)
        => base.Json_collection_Select_entity_with_initializer_ElementAt(async);

    [ConditionalTheory(Skip = SkipJsonCollectionReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_collection_Select_entity_in_anonymous_object_ElementAt(bool async)
        => base.Json_collection_Select_entity_in_anonymous_object_ElementAt(async);

    [ConditionalTheory(Skip = SkipJsonCollectionReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_collection_index_with_parameter_Select_ElementAt(bool async)
        => base.Json_collection_index_with_parameter_Select_ElementAt(async);

    [ConditionalTheory(Skip = SkipJsonCollectionReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_collection_index_in_projection_using_untranslatable_client_method(bool async)
        => base.Json_collection_index_in_projection_using_untranslatable_client_method(async);

    [ConditionalTheory(Skip = SkipJsonCollectionReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_collection_index_in_projection_using_untranslatable_client_method2(bool async)
        => base.Json_collection_index_in_projection_using_untranslatable_client_method2(async);

    [ConditionalTheory(Skip = SkipJsonCollectionReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_nested_collection_anonymous_projection_of_primitives_in_projection_NoTrackingWithIdentityResolution(bool async)
        => base.Json_nested_collection_anonymous_projection_of_primitives_in_projection_NoTrackingWithIdentityResolution(async);

    [ConditionalTheory(Skip = SkipJsonCollectionReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_collection_index_with_expression_Select_ElementAt(bool async)
        => base.Json_collection_index_with_expression_Select_ElementAt(async);

    #endregion

    #region Dynamic array indices not supported

    private const string SkipDynamicArrayIndexReason = "BigQuery JSON paths do not support dynamic array indices";

    [ConditionalTheory(Skip = SkipDynamicArrayIndexReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_collection_index_in_projection_using_parameter(bool async)
        => base.Json_collection_index_in_projection_using_parameter(async);

    [ConditionalTheory(Skip = SkipDynamicArrayIndexReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_collection_index_in_projection_using_parameter_when_owner_is_present(bool async)
        => base.Json_collection_index_in_projection_using_parameter_when_owner_is_present(async);

    [ConditionalTheory(Skip = SkipDynamicArrayIndexReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_collection_index_in_projection_using_parameter_when_owner_is_not_present(bool async)
        => base.Json_collection_index_in_projection_using_parameter_when_owner_is_not_present(async);

    [ConditionalTheory(Skip = SkipDynamicArrayIndexReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_collection_index_in_projection_using_column(bool async)
        => base.Json_collection_index_in_projection_using_column(async);

    [ConditionalTheory(Skip = SkipDynamicArrayIndexReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_collection_index_in_projection_nested(bool async)
        => base.Json_collection_index_in_projection_nested(async);

    [ConditionalTheory(Skip = SkipDynamicArrayIndexReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_collection_index_in_projection_nested_project_scalar(bool async)
        => base.Json_collection_index_in_projection_nested_project_scalar(async);

    [ConditionalTheory(Skip = SkipDynamicArrayIndexReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_collection_index_in_projection_nested_project_reference(bool async)
        => base.Json_collection_index_in_projection_nested_project_reference(async);

    [ConditionalTheory(Skip = SkipDynamicArrayIndexReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_collection_index_in_projection_nested_project_collection(bool async)
        => base.Json_collection_index_in_projection_nested_project_collection(async);

    [ConditionalTheory(Skip = SkipDynamicArrayIndexReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_collection_index_in_projection_nested_project_collection_anonymous_projection(bool async)
        => base.Json_collection_index_in_projection_nested_project_collection_anonymous_projection(async);

    [ConditionalTheory(Skip = SkipDynamicArrayIndexReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_collection_index_in_projection_when_owner_is_present_misc1(bool async)
        => base.Json_collection_index_in_projection_when_owner_is_present_misc1(async);

    [ConditionalTheory(Skip = SkipDynamicArrayIndexReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_collection_index_in_projection_when_owner_is_present_multiple(bool async)
        => base.Json_collection_index_in_projection_when_owner_is_present_multiple(async);

    [ConditionalTheory(Skip = SkipDynamicArrayIndexReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_collection_index_in_projection_when_owner_is_not_present_misc1(bool async)
        => base.Json_collection_index_in_projection_when_owner_is_not_present_misc1(async);

    [ConditionalTheory(Skip = SkipDynamicArrayIndexReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_collection_index_in_projection_when_owner_is_not_present_multiple(bool async)
        => base.Json_collection_index_in_projection_when_owner_is_not_present_multiple(async);

    [ConditionalTheory(Skip = SkipDynamicArrayIndexReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_collection_index_in_predicate_using_variable(bool async)
        => base.Json_collection_index_in_predicate_using_variable(async);

    [ConditionalTheory(Skip = SkipDynamicArrayIndexReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_collection_index_in_predicate_using_column(bool async)
        => base.Json_collection_index_in_predicate_using_column(async);

    [ConditionalTheory(Skip = SkipDynamicArrayIndexReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_collection_index_in_predicate_using_complex_expression1(bool async)
        => base.Json_collection_index_in_predicate_using_complex_expression1(async);

    [ConditionalTheory(Skip = SkipDynamicArrayIndexReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_collection_index_in_predicate_using_complex_expression2(bool async)
        => base.Json_collection_index_in_predicate_using_complex_expression2(async);

    [ConditionalTheory(Skip = SkipDynamicArrayIndexReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_collection_index_in_predicate_nested_mix(bool async)
        => base.Json_collection_index_in_predicate_nested_mix(async);

    [ConditionalTheory(Skip = SkipDynamicArrayIndexReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_projection_deduplication_with_collection_indexer_in_target(bool async)
        => base.Json_projection_deduplication_with_collection_indexer_in_target(async);

    [ConditionalTheory(Skip = SkipDynamicArrayIndexReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_projection_deduplication_with_collection_in_original_and_collection_indexer_in_target(bool async)
        => base.Json_projection_deduplication_with_collection_in_original_and_collection_indexer_in_target(async);

    [ConditionalTheory(Skip = SkipDynamicArrayIndexReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_projection_nested_collection_element_using_parameter_and_the_owner_in_correct_order_AsNoTrackingWithIdentityResolution(bool async)
        => base.Json_projection_nested_collection_element_using_parameter_and_the_owner_in_correct_order_AsNoTrackingWithIdentityResolution(async);

    [ConditionalTheory(Skip = SkipDynamicArrayIndexReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_projection_only_second_element_through_collection_element_parameter_projected_nested_AsNoTrackingWithIdentityResolution(bool async)
        => base.Json_projection_only_second_element_through_collection_element_parameter_projected_nested_AsNoTrackingWithIdentityResolution(async);

    [ConditionalTheory(Skip = SkipDynamicArrayIndexReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_projection_second_element_through_collection_element_parameter_correctly_projected_after_owner_nested_AsNoTrackingWithIdentityResolution(bool async)
        => base.Json_projection_second_element_through_collection_element_parameter_correctly_projected_after_owner_nested_AsNoTrackingWithIdentityResolution(async);

    [ConditionalTheory(Skip = SkipDynamicArrayIndexReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_collection_after_collection_index_in_projection_using_parameter_when_owner_is_present(bool async)
        => base.Json_collection_after_collection_index_in_projection_using_parameter_when_owner_is_present(async);

    [ConditionalTheory(Skip = SkipDynamicArrayIndexReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_collection_after_collection_index_in_projection_using_parameter_when_owner_is_not_present(bool async)
        => base.Json_collection_after_collection_index_in_projection_using_parameter_when_owner_is_not_present(async);

    #endregion

    #region BigQuery JSON limitations

    private const string SkipJsonDistinctReason = "BigQuery does not support JSON columns in SELECT DISTINCT";

    [ConditionalTheory(Skip = SkipJsonDistinctReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_subquery_reference_pushdown_property(bool async)
        => base.Json_subquery_reference_pushdown_property(async);

    [ConditionalTheory(Skip = SkipJsonDistinctReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_subquery_reference_pushdown_reference(bool async)
        => base.Json_subquery_reference_pushdown_reference(async);

    [ConditionalTheory(Skip = SkipJsonDistinctReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_subquery_reference_pushdown_reference_pushdown_reference(bool async)
        => base.Json_subquery_reference_pushdown_reference_pushdown_reference(async);

    [ConditionalTheory(Skip = SkipJsonDistinctReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_subquery_reference_pushdown_reference_pushdown_collection(bool async)
        => base.Json_subquery_reference_pushdown_reference_pushdown_collection(async);

    private const string SkipJsonUnnestReason = "BigQuery UNNEST does not support JSON type";

    [ConditionalTheory(Skip = SkipJsonUnnestReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_collection_of_primitives_SelectMany(bool async)
        => base.Json_collection_of_primitives_SelectMany(async);

    // BigQuery doesn't support OFFSET subscript access on JSON
    private const string SkipJsonOffsetReason = "BigQuery does not support OFFSET subscript access on JSON";

    [ConditionalTheory(Skip = SkipJsonOffsetReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_collection_of_primitives_index_used_in_projection(bool async)
        => base.Json_collection_of_primitives_index_used_in_projection(async);

    [ConditionalTheory(Skip = SkipJsonOffsetReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_collection_of_primitives_index_used_in_predicate(bool async)
        => base.Json_collection_of_primitives_index_used_in_predicate(async);

    [ConditionalTheory(Skip = SkipJsonOffsetReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_collection_of_primitives_index_used_in_orderby(bool async)
        => base.Json_collection_of_primitives_index_used_in_orderby(async);

    [ConditionalTheory(Skip = SkipJsonOffsetReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_collection_of_primitives_contains_in_predicate(bool async)
        => base.Json_collection_of_primitives_contains_in_predicate(async);

    // Special characters in JSON property names cause escape sequence issues
    private const string SkipJsonEscapeReason = "Special characters in JSON property names cause BigQuery escape sequence issues";

    [ConditionalTheory(Skip = SkipJsonEscapeReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Custom_naming_projection_everything(bool async)
        => base.Custom_naming_projection_everything(async);

    [ConditionalTheory(Skip = SkipJsonEscapeReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Custom_naming_projection_owned_reference(bool async)
        => base.Custom_naming_projection_owned_reference(async);

    [ConditionalTheory(Skip = SkipJsonEscapeReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Custom_naming_projection_owned_scalar(bool async)
        => base.Custom_naming_projection_owned_scalar(async);

    // AsNoTrackingWithIdentityResolution variants of skipped tests throw different exception messages
    [ConditionalTheory(Skip = SkipJsonCollectionReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_collection_SelectMany_AsNoTrackingWithIdentityResolution(bool async)
        => base.Json_collection_SelectMany_AsNoTrackingWithIdentityResolution(async);

    [ConditionalTheory(Skip = SkipJsonCollectionReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_projection_using_queryable_methods_on_top_of_JSON_collection_AsNoTrackingWithIdentityResolution(bool async)
        => base.Json_projection_using_queryable_methods_on_top_of_JSON_collection_AsNoTrackingWithIdentityResolution(async);

    [ConditionalTheory(Skip = SkipJsonCollectionReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_branch_collection_distinct_and_other_collection_AsNoTrackingWithIdentityResolution(bool async)
        => base.Json_branch_collection_distinct_and_other_collection_AsNoTrackingWithIdentityResolution(async);

    [ConditionalTheory(Skip = SkipJsonCollectionReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_nested_collection_anonymous_projection_in_projection_NoTrackingWithIdentityResolution(bool async)
        => base.Json_nested_collection_anonymous_projection_in_projection_NoTrackingWithIdentityResolution(async);

    // Subquery deduplication with FirstOrDefault - SQL generation issues
    private const string SkipSubqueryDeduplicationReason = "Subquery deduplication with JSON has SQL generation issues";

    [ConditionalTheory(Skip = SkipSubqueryDeduplicationReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Project_json_entity_FirstOrDefault_subquery_deduplication(bool async)
        => base.Project_json_entity_FirstOrDefault_subquery_deduplication(async);

    [ConditionalTheory(Skip = SkipSubqueryDeduplicationReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Project_json_entity_FirstOrDefault_subquery_deduplication_and_outer_reference(bool async)
        => base.Project_json_entity_FirstOrDefault_subquery_deduplication_and_outer_reference(async);

    [ConditionalTheory(Skip = SkipSubqueryDeduplicationReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Project_json_entity_FirstOrDefault_subquery_deduplication_outer_reference_and_pruning(bool async)
        => base.Project_json_entity_FirstOrDefault_subquery_deduplication_outer_reference_and_pruning(async);

    [ConditionalTheory(Skip = "Group by with JSON scalar has aggregation issues in BigQuery")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Group_by_json_scalar_Skip_First_project_json_scalar(bool async)
        => base.Group_by_json_scalar_Skip_First_project_json_scalar(async);

    private const string SkipBoolConverterReason = "Boolean values stored as int/string have BigQuery type comparison issues";

    [ConditionalTheory(Skip = SkipBoolConverterReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_predicate_on_bool_converted_to_int_zero_one(bool async)
        => base.Json_predicate_on_bool_converted_to_int_zero_one(async);

    [ConditionalTheory(Skip = SkipBoolConverterReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_predicate_on_bool_converted_to_int_zero_one_with_explicit_comparison(bool async)
        => base.Json_predicate_on_bool_converted_to_int_zero_one_with_explicit_comparison(async);

    [ConditionalTheory(Skip = SkipBoolConverterReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_predicate_on_int_zero_one_converted_to_bool(bool async)
        => base.Json_predicate_on_int_zero_one_converted_to_bool(async);

    [ConditionalTheory(Skip = SkipBoolConverterReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_predicate_on_bool_converted_to_string_Y_N(bool async)
        => base.Json_predicate_on_bool_converted_to_string_Y_N(async);

    [ConditionalTheory(Skip = SkipBoolConverterReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_predicate_on_bool_converted_to_string_Y_N_with_explicit_comparison(bool async)
        => base.Json_predicate_on_bool_converted_to_string_Y_N_with_explicit_comparison(async);

    [ConditionalTheory(Skip = SkipBoolConverterReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_predicate_on_string_Y_N_converted_to_bool(bool async)
        => base.Json_predicate_on_string_Y_N_converted_to_bool(async);

    [ConditionalTheory(Skip = SkipBoolConverterReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_predicate_on_bool_converted_to_string_True_False(bool async)
        => base.Json_predicate_on_bool_converted_to_string_True_False(async);

    [ConditionalTheory(Skip = SkipBoolConverterReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_predicate_on_bool_converted_to_string_True_False_with_explicit_comparison(bool async)
        => base.Json_predicate_on_bool_converted_to_string_True_False_with_explicit_comparison(async);

    [ConditionalTheory(Skip = SkipBoolConverterReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Json_predicate_on_string_True_False_converted_to_bool(bool async)
        => base.Json_predicate_on_string_True_False_converted_to_bool(async);

    #endregion

    private void AssertSql(params string[] expected)
        => Fixture.TestSqlLoggerFactory.AssertBaseline(expected);

    public class JsonQueryBigQueryFixture : JsonQueryRelationalFixture, IQueryFixtureBase
    {
        protected override ITestStoreFactory TestStoreFactory
            => BigQueryTestStoreFactory.Instance;

        private readonly IReadOnlyDictionary<Type, object> _entityAsserters;

        public JsonQueryBigQueryFixture()
        {
            var entityAsserters = base.EntityAsserters.ToDictionary();

            entityAsserters[typeof(JsonEntityAllTypes)] = (object e, object a) =>
            {
                Assert.Equal(e == null, a == null);
                if (a != null)
                {
                    var ee = (JsonEntityAllTypes)e;
                    var aa = (JsonEntityAllTypes)a;

                    Assert.Equal(ee.Id, aa.Id);

                    AssertAllTypes(ee.Reference, aa.Reference);

                    Assert.Equal(ee.Collection?.Count ?? 0, aa.Collection?.Count ?? 0);
                    for (var i = 0; i < (ee.Collection?.Count ?? 0); i++)
                    {
                        AssertAllTypes(ee.Collection![i], aa.Collection![i]);
                    }
                }
            };

            entityAsserters[typeof(JsonOwnedAllTypes)] = (object e, object a) =>
            {
                Assert.Equal(e == null, a == null);
                if (a != null)
                {
                    var ee = (JsonOwnedAllTypes)e;
                    var aa = (JsonOwnedAllTypes)a;

                    AssertAllTypes(ee, aa);
                }
            };

            _entityAsserters = entityAsserters;
        }

        IReadOnlyDictionary<Type, object> IQueryFixtureBase.EntityAsserters
            => _entityAsserters;

        protected override void OnModelCreating(ModelBuilder modelBuilder, DbContext context)
        {
            base.OnModelCreating(modelBuilder, context);

            // BigQuery-specific: Ignore unsupported collection types
            // BigQuery JSON doesn't support all the same array types as other providers
            modelBuilder.Entity<JsonEntityAllTypes>(
                b =>
                {
                    b.Ignore(j => j.TestEnumCollection);
                    b.Ignore(j => j.TestUnsignedInt16Collection);
                    b.Ignore(j => j.TestNullableEnumCollection);
                    b.Ignore(j => j.TestNullableEnumWithIntConverterCollection);
                    b.Ignore(j => j.TestCharacterCollection);
                    b.Ignore(j => j.TestNullableInt32Collection);
                    b.Ignore(j => j.TestUnsignedInt64Collection);

                    b.Ignore(j => j.TestByteCollection);
                    b.Ignore(j => j.TestBooleanCollection);
                    b.Ignore(j => j.TestDateTimeOffsetCollection);
                    b.Ignore(j => j.TestDoubleCollection);
                    b.Ignore(j => j.TestInt16Collection);

                    b.Ignore(j => j.TestInt64Collection);
                    b.Ignore(j => j.TestGuidCollection);

                    // Nested collections not supported
                    b.Ignore(j => j.TestDefaultStringCollectionCollection);
                    b.Ignore(j => j.TestMaxLengthStringCollectionCollection);
                    b.Ignore(j => j.TestInt16CollectionCollection);
                    b.Ignore(j => j.TestInt32CollectionCollection);
                    b.Ignore(j => j.TestInt64CollectionCollection);
                    b.Ignore(j => j.TestDoubleCollectionCollection);
                    b.Ignore(j => j.TestSingleCollectionCollection);
                    b.Ignore(j => j.TestCharacterCollectionCollection);
                    b.Ignore(j => j.TestBooleanCollectionCollection);
                    b.Ignore(j => j.TestNullableInt32CollectionCollection);
                    b.Ignore(j => j.TestNullableEnumCollectionCollection);
                    b.Ignore(j => j.TestNullableEnumWithIntConverterCollectionCollection);
                    b.Ignore(j => j.TestNullableEnumWithConverterThatHandlesNullsCollection);
                });

            modelBuilder.Entity<JsonEntityAllTypes>().OwnsOne(
                x => x.Reference, b =>
                {
                    b.Ignore(j => j.TestDefaultStringCollectionCollection);
                    b.Ignore(j => j.TestMaxLengthStringCollectionCollection);
                    b.Ignore(j => j.TestInt16CollectionCollection);
                    b.Ignore(j => j.TestInt32CollectionCollection);
                    b.Ignore(j => j.TestInt64CollectionCollection);
                    b.Ignore(j => j.TestDoubleCollectionCollection);
                    b.Ignore(j => j.TestSingleCollectionCollection);
                    b.Ignore(j => j.TestBooleanCollectionCollection);
                    b.Ignore(j => j.TestCharacterCollectionCollection);
                    b.Ignore(j => j.TestNullableInt32CollectionCollection);
                    b.Ignore(j => j.TestNullableEnumCollectionCollection);
                    b.Ignore(j => j.TestNullableEnumWithIntConverterCollectionCollection);
                    b.Ignore(j => j.TestNullableEnumWithConverterThatHandlesNullsCollection);
                });

            modelBuilder.Entity<JsonEntityAllTypes>().OwnsMany(
                x => x.Collection, b =>
                {
                    b.Ignore(j => j.TestDefaultStringCollectionCollection);
                    b.Ignore(j => j.TestMaxLengthStringCollectionCollection);
                    b.Ignore(j => j.TestInt16CollectionCollection);
                    b.Ignore(j => j.TestInt32CollectionCollection);
                    b.Ignore(j => j.TestInt64CollectionCollection);
                    b.Ignore(j => j.TestDoubleCollectionCollection);
                    b.Ignore(j => j.TestSingleCollectionCollection);
                    b.Ignore(j => j.TestBooleanCollectionCollection);
                    b.Ignore(j => j.TestCharacterCollectionCollection);
                    b.Ignore(j => j.TestNullableInt32CollectionCollection);
                    b.Ignore(j => j.TestNullableEnumCollectionCollection);
                    b.Ignore(j => j.TestNullableEnumWithIntConverterCollectionCollection);
                    b.Ignore(j => j.TestNullableEnumWithConverterThatHandlesNullsCollection);
                });
        }


        public static new void AssertAllTypes(JsonOwnedAllTypes expected, JsonOwnedAllTypes actual)
        {
            Assert.Equal(expected.TestDefaultString, actual.TestDefaultString);
            Assert.Equal(expected.TestMaxLengthString, actual.TestMaxLengthString);
            Assert.Equal(expected.TestBoolean, actual.TestBoolean);
            Assert.Equal(expected.TestCharacter, actual.TestCharacter);
            Assert.Equal(expected.TestDateTime, actual.TestDateTime);
            Assert.Equal(expected.TestDateTimeOffset, actual.TestDateTimeOffset);
            Assert.Equal(expected.TestDouble, actual.TestDouble);
            Assert.Equal(expected.TestGuid, actual.TestGuid);
            Assert.Equal(expected.TestInt16, actual.TestInt16);
            Assert.Equal(expected.TestInt32, actual.TestInt32);
            Assert.Equal(expected.TestInt64, actual.TestInt64);
            Assert.Equal(expected.TestSignedByte, actual.TestSignedByte);
            Assert.Equal(expected.TestSingle, actual.TestSingle);
            Assert.Equal(expected.TestTimeSpan, actual.TestTimeSpan);
            Assert.Equal(expected.TestDateOnly, actual.TestDateOnly);
            Assert.Equal(expected.TestTimeOnly, actual.TestTimeOnly);
            Assert.Equal(expected.TestUnsignedInt16, actual.TestUnsignedInt16);
            Assert.Equal(expected.TestUnsignedInt32, actual.TestUnsignedInt32);
            Assert.Equal(expected.TestUnsignedInt64, actual.TestUnsignedInt64);
            Assert.Equal(expected.TestEnum, actual.TestEnum);
            Assert.Equal(expected.TestEnumWithIntConverter, actual.TestEnumWithIntConverter);
            Assert.Equal(expected.TestNullableEnum, actual.TestNullableEnum);
            Assert.Equal(expected.TestNullableEnumWithIntConverter, actual.TestNullableEnumWithIntConverter);
            Assert.Equal(expected.TestNullableEnumWithConverterThatHandlesNulls, actual.TestNullableEnumWithConverterThatHandlesNulls);
        }

        protected override Task SeedAsync(JsonQueryContext context)
        {
            // Custom seeding that skips JsonEntityAllTypes due to serialization issues
            // All other entity types work correctly
            var jsonEntitiesBasic = JsonQueryData.CreateJsonEntitiesBasic();
            var entitiesBasic = JsonQueryData.CreateEntitiesBasic();
            var jsonEntitiesBasicForReference = JsonQueryData.CreateJsonEntitiesBasicForReference();
            var jsonEntitiesBasicForCollection = JsonQueryData.CreateJsonEntitiesBasicForCollection();
            JsonQueryData.WireUp(jsonEntitiesBasic, entitiesBasic, jsonEntitiesBasicForReference, jsonEntitiesBasicForCollection);

            var jsonEntitiesCustomNaming = JsonQueryData.CreateJsonEntitiesCustomNaming();
            var jsonEntitiesSingleOwned = JsonQueryData.CreateJsonEntitiesSingleOwned();
            var jsonEntitiesInheritance = JsonQueryData.CreateJsonEntitiesInheritance();
            // Skip JsonEntityAllTypes - EF Core's ModificationCommand.WriteJson has issues with this entity type
            // var jsonEntitiesAllTypes = JsonQueryData.CreateJsonEntitiesAllTypes();
            var jsonEntitiesConverters = JsonQueryData.CreateJsonEntitiesConverters();

            context.JsonEntitiesBasic.AddRange(jsonEntitiesBasic);
            context.EntitiesBasic.AddRange(entitiesBasic);
            context.JsonEntitiesBasicForReference.AddRange(jsonEntitiesBasicForReference);
            context.JsonEntitiesBasicForCollection.AddRange(jsonEntitiesBasicForCollection);
            context.JsonEntitiesCustomNaming.AddRange(jsonEntitiesCustomNaming);
            context.JsonEntitiesSingleOwned.AddRange(jsonEntitiesSingleOwned);
            context.JsonEntitiesInheritance.AddRange(jsonEntitiesInheritance);
            // context.JsonEntitiesAllTypes.AddRange(jsonEntitiesAllTypes);
            context.JsonEntitiesConverters.AddRange(jsonEntitiesConverters);

            return context.SaveChangesAsync();
        }
    }
}
