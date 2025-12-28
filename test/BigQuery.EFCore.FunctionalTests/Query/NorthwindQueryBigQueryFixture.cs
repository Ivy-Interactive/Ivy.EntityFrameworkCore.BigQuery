using Ivy.EntityFrameworkCore.BigQuery.TestUtilities;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace Ivy.EntityFrameworkCore.BigQuery.Query;

public class NorthwindQueryBigQueryFixture<TModelCustomizer> : NorthwindQueryRelationalFixture<TModelCustomizer>
    where TModelCustomizer : ITestModelCustomizer, new()
{
    protected override ITestStoreFactory TestStoreFactory
        => BigQueryNorthwindTestStoreFactory.Instance;

    protected override Type ContextType
        => typeof(TestModels.Northwind.NorthwindBigQueryContext);

    // Return the dataset name from the connection string, not "Northwind"
    // This allows tests that reference Fixture.TestStore.Name in SQL to use the correct dataset
    protected override string StoreName
    {
        get
        {
            var connStr = Environment.GetEnvironmentVariable("BQ_EFCORE_TEST_CONN_STRING");
            if (!string.IsNullOrEmpty(connStr))
            {
                var builder = new Ivy.Data.BigQuery.BigQueryConnectionStringBuilder(connStr);
                if (!string.IsNullOrEmpty(builder.DefaultDatasetId))
                {
                    return builder.DefaultDatasetId;
                }
            }
            return base.StoreName;
        }
    }
}

public class NorthwindQueryBigQueryFixture : NorthwindQueryBigQueryFixture<BigQueryNorthwindModelCustomizer>
{
}