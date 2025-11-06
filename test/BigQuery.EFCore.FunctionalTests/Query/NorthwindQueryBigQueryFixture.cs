using Ivy.EntityFrameworkCore.BigQuery.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace Ivy.EntityFrameworkCore.BigQuery.FunctionalTests.Query
{
    public class NorthwindQueryBigQueryFixture<TModelCustomizer> : NorthwindQueryRelationalFixture<TModelCustomizer>
        where TModelCustomizer : ITestModelCustomizer, new()
    {
        protected override ITestStoreFactory TestStoreFactory
            => BigQueryNorthwindTestStoreFactory.Instance;

        protected override Type ContextType
            => typeof(TestModels.Northwind.NorthwindBigQueryContext);
    }

    public class NorthwindQueryBigQueryFixture : NorthwindQueryBigQueryFixture<BigQueryNorthwindModelCustomizer>
    {
    }
}