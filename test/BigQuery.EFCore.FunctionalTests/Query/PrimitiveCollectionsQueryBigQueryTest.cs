using Ivy.EntityFrameworkCore.BigQuery.TestUtilities;
using Microsoft.EntityFrameworkCore;
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

    public class PrimitiveCollectionsQueryBigQueryFixture : PrimitiveCollectionsQueryFixtureBase, ITestSqlLoggerFactory
    {
        private BigQueryPrimitiveCollectionsData? _expectedData;

        public TestSqlLoggerFactory TestSqlLoggerFactory
            => (TestSqlLoggerFactory)ListLoggerFactory;

        protected override ITestStoreFactory TestStoreFactory
            => BigQueryTestStoreFactory.Instance;

        // Disable pooling to avoid service provider issues with relational facade dependencies
        protected override bool UsePooling => false;

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