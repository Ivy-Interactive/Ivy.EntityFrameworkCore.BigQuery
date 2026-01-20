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
SELECT `j`.`OwnedReferenceRoot`.OwnedReferenceBranch, `j`.`Id`
FROM `JsonEntitiesBasic` AS `j`
""");
    }

    public override async Task Basic_json_projection_owned_collection_branch(bool async)
    {
        await base.Basic_json_projection_owned_collection_branch(async);

        AssertSql(
            """
SELECT `j`.`OwnedReferenceRoot`.OwnedCollectionBranch, `j`.`Id`
FROM `JsonEntitiesBasic` AS `j`
""");
    }

    public override async Task Basic_json_projection_owned_reference_leaf(bool async)
    {
        await base.Basic_json_projection_owned_reference_leaf(async);

        AssertSql(
            """
SELECT `j`.`OwnedReferenceRoot`.OwnedReferenceBranch.OwnedReferenceLeaf, `j`.`Id`
FROM `JsonEntitiesBasic` AS `j`
""");
    }

    public override async Task Basic_json_projection_owned_collection_leaf(bool async)
    {
        await base.Basic_json_projection_owned_collection_leaf(async);

        AssertSql(
            """
SELECT `j`.`OwnedReferenceRoot`.OwnedReferenceBranch.OwnedCollectionLeaf, `j`.`Id`
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
SELECT `j`.`Name`, LENGTH(STRING(`j`.`OwnedReferenceRoot`.Name)) AS `JsonScalarLength`
FROM `JsonEntitiesBasic` AS `j`
WHERE LENGTH(STRING(`j`.`OwnedReferenceRoot`.Name)) > 2
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
WHERE INT64(`j`.`OwnedReferenceRoot`.OwnedReferenceBranch.Fraction) < 20.5
""");
    }

    #endregion

    #region Enum

    public override async Task Basic_json_projection_enum_inside_json_entity(bool async)
    {
        await base.Basic_json_projection_enum_inside_json_entity(async);

        AssertSql(
            """
SELECT `j`.`Id`, INT64(`j`.`OwnedReferenceRoot`.OwnedReferenceBranch.Enum) AS `Enum`
FROM `JsonEntitiesBasic` AS `j`
""");
    }

    public override async Task Json_projection_enum_with_custom_conversion(bool async)
    {
        await base.Json_projection_enum_with_custom_conversion(async);

        AssertSql(
            """
SELECT `j`.`Id`, INT64(`j`.`OwnedReferenceRoot`.OwnedReferenceBranch.Enum) AS `Enum`
FROM `JsonEntitiesBasic` AS `j`
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
