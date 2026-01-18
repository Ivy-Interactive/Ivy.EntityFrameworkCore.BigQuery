using Ivy.EntityFrameworkCore.BigQuery.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit.Abstractions;

namespace Ivy.EntityFrameworkCore.BigQuery;

public class BuiltInDataTypesBigQueryTest : BuiltInDataTypesTestBase<BuiltInDataTypesBigQueryTest.BuiltInDataTypesBigQueryFixture>
{
    public BuiltInDataTypesBigQueryTest(BuiltInDataTypesBigQueryFixture fixture, ITestOutputHelper testOutputHelper)
   : base(fixture)
    {
        Fixture.TestSqlLoggerFactory.Clear();
        Fixture.TestSqlLoggerFactory.SetTestOutputHelper(testOutputHelper);
    }

    // BigQuery doesn't support BYTES type as primary key
    public override Task Can_insert_and_read_back_with_null_binary_foreign_key()
        => Task.CompletedTask;

    // No No DateTimeOffset in BigQuery
    public override Task Can_query_with_null_parameters_using_any_nullable_data_type()
        => Task.CompletedTask;

    public class BuiltInDataTypesBigQueryFixture : BuiltInDataTypesFixtureBase, ITestSqlLoggerFactory
    {
        public override bool StrictEquality
            => false;

        public override bool SupportsAnsi
            => false;

        public override bool SupportsUnicodeToAnsiConversion
            => true;

        public override bool SupportsLargeStringComparisons
            => true;

        public override bool SupportsDecimalComparisons
            => false;

        public override bool PreservesDateTimeKind
            => false;

        protected override ITestStoreFactory TestStoreFactory
            => BigQueryTestStoreFactory.Instance;

        public TestSqlLoggerFactory TestSqlLoggerFactory
            => (TestSqlLoggerFactory)ListLoggerFactory;

        protected override void OnModelCreating(ModelBuilder modelBuilder, DbContext context)
        {
            base.OnModelCreating(modelBuilder, context);

            // BigQuery doesn't support BYTES type as primary key
            modelBuilder.Ignore<BinaryKeyDataType>();
            modelBuilder.Ignore<BinaryForeignKeyDataType>();

            // BigQuery doesn't have unsigned integer types - ignore them like Npgsql does
            // BigQuery TIMESTAMP is always UTC and maps to DateTime, not DateTimeOffset
            // BigQuery TIME returns as TimeSpan, not TimeOnly - need value converter to support
            // BigQuery DATE returns as DateTime, not DateOnly - need value converter to support
            modelBuilder.Entity<BuiltInDataTypes>(b =>
            {
                b.Ignore(dt => dt.TestUnsignedInt16);
                b.Ignore(dt => dt.TestUnsignedInt32);
                b.Ignore(dt => dt.TestUnsignedInt64);
                b.Ignore(dt => dt.TestDateTimeOffset);
                b.Ignore(dt => dt.TestTimeOnly);
                b.Ignore(dt => dt.TestDateOnly);
            });

            modelBuilder.Entity<BuiltInNullableDataTypes>(b =>
            {
                b.Ignore(dt => dt.TestNullableUnsignedInt16);
                b.Ignore(dt => dt.TestNullableUnsignedInt32);
                b.Ignore(dt => dt.TestNullableUnsignedInt64);
                b.Ignore(dt => dt.TestNullableDateTimeOffset);
                b.Ignore(dt => dt.TestNullableTimeOnly);
                b.Ignore(dt => dt.TestNullableDateOnly);
            });

            // Other entity types that have unsupported types
            modelBuilder.Entity<ObjectBackedDataTypes>(b =>
            {
                b.Ignore(e => e.DateTimeOffset);
                b.Ignore(e => e.TimeOnly);
                b.Ignore(e => e.DateOnly);
            });
            modelBuilder.Entity<NullableBackedDataTypes>(b =>
            {
                b.Ignore(e => e.DateTimeOffset);
                b.Ignore(e => e.TimeOnly);
                b.Ignore(e => e.DateOnly);
            });
            modelBuilder.Entity<NonNullableBackedDataTypes>(b =>
            {
                b.Ignore(e => e.DateTimeOffset);
                b.Ignore(e => e.TimeOnly);
                b.Ignore(e => e.DateOnly);
            });
            modelBuilder.Entity<DateTimeEnclosure>().Ignore(e => e.DateTimeOffset);

            // Shadow entities get properties copied from the main entities - need to ignore these shadow properties too
            var shadowEntityTypes = new[] { typeof(BuiltInDataTypesShadow), typeof(BuiltInNullableDataTypesShadow) };
            var ignoredProperties = new[]
            {
                "TestUnsignedInt16", "TestUnsignedInt32", "TestUnsignedInt64",
                "TestDateTimeOffset", "TestTimeOnly", "TestDateOnly",
                "TestNullableUnsignedInt16", "TestNullableUnsignedInt32", "TestNullableUnsignedInt64",
                "TestNullableDateTimeOffset", "TestNullableTimeOnly", "TestNullableDateOnly"
            };

            foreach (var entityType in shadowEntityTypes)
            {
                var entityBuilder = modelBuilder.Entity(entityType);
                var metadata = entityBuilder.Metadata;
                foreach (var propName in ignoredProperties)
                {
                    var prop = metadata.FindProperty(propName);
                    if (prop != null)
                    {
                        metadata.RemoveProperty(prop);
                    }
                }
            }
        }

        public override bool SupportsBinaryKeys
            => false;

        public override DateTime DefaultDateTime
            => new();
    }
}
