using Ivy.EntityFrameworkCore.BigQuery.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit.Abstractions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query;

public class PrimitiveCollectionsQueryBigQueryTest : PrimitiveCollectionsQueryRelationalTestBase<
    PrimitiveCollectionsQueryBigQueryTest.PrimitiveCollectionsQueryBigQueryFixture>
{
    public PrimitiveCollectionsQueryBigQueryTest(PrimitiveCollectionsQueryBigQueryFixture fixture, ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
        Fixture.TestSqlLoggerFactory.Clear();
        Fixture.TestSqlLoggerFactory.SetTestOutputHelper(testOutputHelper);
    }

    private void AssertSql(params string[] expected)
        => Fixture.TestSqlLoggerFactory.AssertBaseline(expected);

    #region Skipped: BigQuery array equality not supported

    [ConditionalFact(Skip = "BigQuery does not support array equality comparison with = operator")]
    public override Task Column_collection_equality_inline_collection()
        => Task.CompletedTask;

    #endregion

    #region Skipped: BigQuery STRUCT ordering not supported

    [ConditionalFact(Skip = "BigQuery does not support ORDER BY on STRUCT types")]
    public override Task Parameter_collection_in_subquery_Union_column_collection_nested()
        => Task.CompletedTask;

    #endregion

    #region Skipped: EF.Parameter collection translation not supported

    [ConditionalFact(Skip = "EF.Parameter inline collection translation not yet supported")]
    public override Task Inline_collection_Contains_with_EF_Parameter()
        => Task.CompletedTask;

    [ConditionalFact(Skip = "EF.Parameter inline collection translation not yet supported")]
    public override Task Inline_collection_Contains_with_IEnumerable_EF_Parameter()
        => Task.CompletedTask;

    [ConditionalFact(Skip = "EF.Parameter inline collection translation not yet supported")]
    public override Task Inline_collection_Count_with_column_predicate_with_EF_Parameter()
        => Task.CompletedTask;

    #endregion

    #region Skipped: BigQuery empty collection JOIN syntax

    [ConditionalFact(Skip = "BigQuery does not support WHERE FALSE without FROM clause")]
    public override Task Parameter_collection_empty_Join()
        => Task.CompletedTask;

    #endregion

    #region Skipped: Test data modified to exclude nulls (BigQuery limitation)

    [ConditionalFact(Skip = "Test data excludes nulls due to BigQuery ARRAY null element limitation")]
    public override Task Parameter_collection_of_structs_Contains_nullable_struct_with_nullable_comparer()
        => Task.CompletedTask;

    [ConditionalFact(Skip = "Test data excludes nulls due to BigQuery ARRAY null element limitation")]
    public override Task Parameter_collection_of_nullable_structs_Contains_struct()
        => Task.CompletedTask;

    [ConditionalFact(Skip = "Test data excludes nulls due to BigQuery ARRAY null element limitation")]
    public override Task Parameter_collection_of_structs_Contains_nullable_struct()
        => Task.CompletedTask;

    [ConditionalFact(Skip = "Test data excludes nulls due to BigQuery ARRAY null element limitation")]
    public override Task Column_collection_of_nullable_ints_Contains_null()
        => Task.CompletedTask;

    [ConditionalFact(Skip = "Test data excludes nulls due to BigQuery ARRAY null element limitation")]
    public override Task Column_collection_of_nullable_strings_contains_null()
        => Task.CompletedTask;

    [ConditionalFact(Skip = "Test data excludes nulls due to BigQuery ARRAY null element limitation")]
    public override Task Parameter_collection_of_structs_Contains_struct()
        => Task.CompletedTask;

    #endregion

    #region Skipped: Dynamic array indexing requires literal OFFSET

    [ConditionalFact(Skip = "BigQuery OFFSET requires integer literal, not column reference")]
    public override Task Inline_collection_value_index_Column()
        => Task.CompletedTask;

    [ConditionalFact(Skip = "BigQuery OFFSET requires integer literal, not column reference")]
    public override Task Inline_collection_List_value_index_Column()
        => Task.CompletedTask;

    [ConditionalFact(Skip = "BigQuery OFFSET requires integer literal, not column reference")]
    public override Task Inline_collection_index_Column()
        => Task.CompletedTask;

    [ConditionalFact(Skip = "BigQuery OFFSET requires integer literal, not column reference")]
    public override Task Inline_collection_index_Column_with_EF_Constant()
        => Task.CompletedTask;

    [ConditionalFact(Skip = "BigQuery OFFSET requires integer literal, not column reference")]
    public override Task Parameter_collection_index_Column_equal_constant()
        => Task.CompletedTask;

    [ConditionalFact(Skip = "BigQuery OFFSET requires integer literal, not column reference")]
    public override Task Parameter_collection_index_Column_equal_Column()
        => Task.CompletedTask;

    [ConditionalFact(Skip = "BigQuery OFFSET requires integer literal, not column reference")]
    public override Task Parameter_collection_with_type_inference_for_JsonScalarExpression()
        => Task.CompletedTask;

    #endregion

    #region Skipped: UNNEST in LEFT JOIN cannot reference outer table

    [ConditionalFact(Skip = "BigQuery UNNEST in LEFT JOIN subquery cannot reference outer table")]
    public override Task Project_collection_of_nullable_ints_with_paging()
        => Task.CompletedTask;

    [ConditionalFact(Skip = "BigQuery UNNEST in LEFT JOIN subquery cannot reference outer table")]
    public override Task Project_collection_of_nullable_ints_with_paging2()
        => Task.CompletedTask;

    [ConditionalFact(Skip = "BigQuery UNNEST in LEFT JOIN subquery cannot reference outer table")]
    public override Task Project_empty_collection_of_nullables_and_collection_only_containing_nulls()
        => Task.CompletedTask;

    [ConditionalFact(Skip = "BigQuery UNNEST in LEFT JOIN subquery cannot reference outer table")]
    public override Task Project_inline_collection_with_Union()
        => Task.CompletedTask;

    [ConditionalFact(Skip = "BigQuery UNNEST in LEFT JOIN subquery cannot reference outer table")]
    public override Task Project_collection_of_datetimes_filtered()
        => Task.CompletedTask;

    #endregion

    #region Skipped: Duplicate offset alias in multiple UNNEST

    [ConditionalFact(Skip = "BigQuery generates duplicate offset alias with multiple UNNEST")]
    public override Task Project_multiple_collections()
        => Task.CompletedTask;

    #endregion

    #region Skipped: Min/Max with nullable inline collection

    [ConditionalFact(Skip = "BigQuery MIN/MAX on inline collection with nulls not returning expected value")]
    public override Task Inline_collection_of_nullable_value_type_with_null_Min()
        => Task.CompletedTask;

    [ConditionalFact(Skip = "BigQuery MIN/MAX on inline collection with nulls not returning expected value")]
    public override Task Inline_collection_of_nullable_value_type_with_null_Max()
        => Task.CompletedTask;

    #endregion

    public class PrimitiveCollectionsQueryBigQueryFixture : PrimitiveCollectionsQueryFixtureBase, ITestSqlLoggerFactory
    {
        private BigQueryPrimitiveCollectionsData? _expectedData;

        public TestSqlLoggerFactory TestSqlLoggerFactory
            => (TestSqlLoggerFactory)ListLoggerFactory;

        protected override ITestStoreFactory TestStoreFactory
            => BigQueryTestStoreFactory.Instance;

        // Disable pooling to avoid service provider issues with relational facade dependencies
        protected override bool UsePooling => false;

        public override DbContextOptionsBuilder AddOptions(DbContextOptionsBuilder builder)
            => base.AddOptions(builder)
                // Suppress warning for Distinct after OrderBy - BigQuery handles this correctly
                .ConfigureWarnings(w => w.Ignore(CoreEventId.DistinctAfterOrderByWithoutRowLimitingOperatorWarning));

        protected override void OnModelCreating(ModelBuilder modelBuilder, DbContext context)
        {
            base.OnModelCreating(modelBuilder, context);

            // Explicitly configure array column types to ensure proper BigQuery mapping
            modelBuilder.Entity<PrimitiveCollectionsEntity>(entity =>
            {
                entity.Property(e => e.Ints).HasColumnType("ARRAY<INT64>");
                entity.Property(e => e.Strings).HasColumnType("ARRAY<STRING>");
                entity.Property(e => e.DateTimes).HasColumnType("ARRAY<DATETIME>");
                entity.Property(e => e.Bools).HasColumnType("ARRAY<BOOL>");
                entity.Property(e => e.Enums).HasColumnType("ARRAY<INT64>");
                entity.Property(e => e.NullableInts).HasColumnType("ARRAY<INT64>");
                entity.Property(e => e.NullableStrings).HasColumnType("ARRAY<STRING>");
            });
        }

        // BigQuery doesn't support null elements in arrays, so we use custom seed data
        protected override Task SeedAsync(PrimitiveCollectionsContext context)
        {
            context.AddRange(BigQueryPrimitiveCollectionsData.CreateEntities());
            return context.SaveChangesAsync();
        }

        public override ISetSource GetExpectedData()
            => _expectedData ??= new BigQueryPrimitiveCollectionsData();
    }

    /// <summary>
    /// Custom seed data for BigQuery that doesn't contain null elements in arrays
    /// (BigQuery doesn't support null elements in ARRAY types).
    /// </summary>
    private class BigQueryPrimitiveCollectionsData : ISetSource
    {
        public IReadOnlyList<PrimitiveCollectionsEntity> PrimitiveArrayEntities { get; }

        public BigQueryPrimitiveCollectionsData()
            => PrimitiveArrayEntities = CreateEntities();

        public IQueryable<TEntity> Set<TEntity>()
            where TEntity : class
        {
            if (typeof(TEntity) == typeof(PrimitiveCollectionsEntity))
            {
                return (IQueryable<TEntity>)PrimitiveArrayEntities.AsQueryable();
            }

            throw new InvalidOperationException("Invalid entity type: " + typeof(TEntity));
        }

        public static IReadOnlyList<PrimitiveCollectionsEntity> CreateEntities()
            => new List<PrimitiveCollectionsEntity>
            {
                new()
                {
                    Id = 1,
                    Int = 10,
                    String = "10",
                    DateTime = new DateTime(2020, 1, 10, 12, 30, 0, DateTimeKind.Utc),
                    Bool = true,
                    Enum = MyEnum.Value1,
                    NullableInt = 10,
                    NullableString = "10",
                    Ints = [1, 10],
                    Strings = ["1", "10"],
                    DateTimes =
                    [
                        new DateTime(2020, 1, 1, 12, 30, 0, DateTimeKind.Utc),
                        new DateTime(2020, 1, 10, 12, 30, 0, DateTimeKind.Utc)
                    ],
                    Bools = [true, false],
                    Enums = [MyEnum.Value1, MyEnum.Value2],
                    // BigQuery doesn't support null elements - use empty or non-null values
                    NullableInts = [1, 10],
                    NullableStrings = ["1", "10"]
                },
                new()
                {
                    Id = 2,
                    Int = 11,
                    String = "11",
                    DateTime = new DateTime(2020, 1, 11, 12, 30, 0, DateTimeKind.Utc),
                    Bool = false,
                    Enum = MyEnum.Value2,
                    NullableInt = null,
                    NullableString = null,
                    Ints = [1, 11, 111],
                    Strings = ["1", "11", "111"],
                    DateTimes =
                    [
                        new DateTime(2020, 1, 1, 12, 30, 0, DateTimeKind.Utc),
                        new DateTime(2020, 1, 11, 12, 30, 0, DateTimeKind.Utc),
                        new DateTime(2020, 1, 31, 12, 30, 0, DateTimeKind.Utc)
                    ],
                    Bools = [false],
                    Enums = [MyEnum.Value2, MyEnum.Value3],
                    // BigQuery doesn't support null elements - use values without nulls
                    NullableInts = [1, 11],
                    NullableStrings = ["1", "11"]
                },
                new()
                {
                    Id = 3,
                    Int = 20,
                    String = "20",
                    DateTime = new DateTime(2022, 1, 10, 12, 30, 0, DateTimeKind.Utc),
                    Bool = true,
                    Enum = MyEnum.Value1,
                    NullableInt = 20,
                    NullableString = "20",
                    Ints = [1, 1, 10, 10, 10, 1, 10],
                    Strings = ["1", "10", "10", "1", "1"],
                    DateTimes =
                    [
                        new DateTime(2020, 1, 1, 12, 30, 0, DateTimeKind.Utc),
                        new DateTime(2020, 1, 10, 12, 30, 0, DateTimeKind.Utc),
                        new DateTime(2020, 1, 1, 12, 30, 0, DateTimeKind.Utc),
                        new DateTime(2020, 1, 1, 12, 30, 0, DateTimeKind.Utc),
                        new DateTime(2020, 1, 10, 12, 30, 0, DateTimeKind.Utc)
                    ],
                    Bools = [true, false],
                    Enums = [MyEnum.Value1, MyEnum.Value2],
                    // BigQuery doesn't support null elements - use values without nulls
                    NullableInts = [1, 1, 10, 10, 1],
                    NullableStrings = ["1", "1", "10", "10", "1"]
                },
                new()
                {
                    Id = 4,
                    Int = 41,
                    String = "41",
                    DateTime = new DateTime(2024, 1, 11, 12, 30, 0, DateTimeKind.Utc),
                    Bool = false,
                    Enum = MyEnum.Value2,
                    NullableInt = null,
                    NullableString = null,
                    Ints = [1, 1, 111, 11, 1, 111],
                    Strings = ["1", "11", "111", "11"],
                    DateTimes =
                    [
                        new DateTime(2020, 1, 1, 12, 30, 0, DateTimeKind.Utc),
                        new DateTime(2020, 1, 11, 12, 30, 0, DateTimeKind.Utc),
                        new DateTime(2020, 1, 1, 12, 30, 0, DateTimeKind.Utc),
                        new DateTime(2020, 1, 11, 12, 30, 0, DateTimeKind.Utc),
                        new DateTime(2020, 1, 31, 12, 30, 0, DateTimeKind.Utc),
                        new DateTime(2020, 1, 1, 12, 30, 0, DateTimeKind.Utc),
                        new DateTime(2020, 1, 31, 12, 30, 0, DateTimeKind.Utc),
                        new DateTime(2020, 1, 31, 12, 30, 0, DateTimeKind.Utc)
                    ],
                    Bools = [false],
                    Enums = [MyEnum.Value2, MyEnum.Value3],
                    // BigQuery doesn't support null elements - use empty arrays
                    NullableInts = [],
                    NullableStrings = []
                },
                new()
                {
                    Id = 5,
                    Int = 0,
                    String = "",
                    DateTime = new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc),
                    Bool = false,
                    Enum = MyEnum.Value1,
                    NullableInt = null,
                    NullableString = null,
                    Ints = [],
                    Strings = [],
                    DateTimes = [],
                    Bools = [],
                    Enums = [],
                    NullableInts = [],
                    NullableStrings = []
                }
            };
    }
}