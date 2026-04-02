using Ivy.EntityFrameworkCore.BigQuery.Extensions;
using Ivy.EntityFrameworkCore.BigQuery.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace Ivy.EntityFrameworkCore.BigQuery;

public class SeedingBigQueryTest : SeedingTestBase
{
    protected override TestStore TestStore
        => BigQueryTestStore.GetOrCreate("SeedingTest");

    protected override SeedingContext CreateContextWithEmptyDatabase(string testId)
        => new SeedingBigQueryContext(testId);

    protected class SeedingBigQueryContext(string testId) : SeedingContext(testId)
    {
        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
            => optionsBuilder.UseBigQuery(BigQueryTestStore.CreateConnectionString($"Seeds{TestId}"));
    }
}