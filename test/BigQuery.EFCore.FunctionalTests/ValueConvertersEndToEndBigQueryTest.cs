using Ivy.EntityFrameworkCore.BigQuery.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace Ivy.EntityFrameworkCore.BigQuery;

public class ValueConvertersEndToEndBigQueryTest(ValueConvertersEndToEndBigQueryTest.ValueConvertersEndToEndBigQueryFixture fixture)
    : ValueConvertersEndToEndTestBase<ValueConvertersEndToEndBigQueryTest.ValueConvertersEndToEndBigQueryFixture>(fixture)
{
    public class ValueConvertersEndToEndBigQueryFixture : ValueConvertersEndToEndFixtureBase
    {
        protected override ITestStoreFactory TestStoreFactory
            => BigQueryTestStoreFactory.Instance;

        protected override void OnModelCreating(ModelBuilder modelBuilder, DbContext context)
        {
            base.OnModelCreating(modelBuilder, context);

            modelBuilder.Entity<ConvertingEntity>(
                b =>
                {
                    // BigQuery TIMESTAMP stores values in UTC, so the original timezone offset is lost on round-trip.
                    // These properties convert string → DateTimeOffset (provider type → TIMESTAMP), losing the offset.
                    b.Ignore(e => e.StringToDateTimeOffset);
                    b.Ignore(e => e.StringToNullableDateTimeOffset);
                    b.Ignore(e => e.NullableStringToDateTimeOffset);
                    b.Ignore(e => e.NullableStringToNullableDateTimeOffset);
                });
        }
    }
}
