using Microsoft.EntityFrameworkCore.Infrastructure;

namespace Ivy.EntityFrameworkCore.BigQuery.TestUtilities;

public static class BigQueryDatabaseFacadeTestExtensions
{
    public static void EnsureClean(this DatabaseFacade databaseFacade)
       => new BigQueryDatabaseCleaner().Clean(databaseFacade);
}