using Ivy.EntityFrameworkCore.BigQuery.Infrastructure;
using Microsoft.EntityFrameworkCore;

namespace Ivy.EntityFrameworkCore.BigQuery.FunctionalTests.TestUtilities;

public static class BigQueryDbContextOptionsBuilderExtensions
{
    public static BigQueryDbContextOptionsBuilder ApplyConfiguration(this BigQueryDbContextOptionsBuilder optionsBuilder)
    {
        optionsBuilder.UseQuerySplittingBehavior(QuerySplittingBehavior.SingleQuery);

        optionsBuilder.CommandTimeout(BigQueryTestStore.CommandTimeout);

        return optionsBuilder;
    }
}