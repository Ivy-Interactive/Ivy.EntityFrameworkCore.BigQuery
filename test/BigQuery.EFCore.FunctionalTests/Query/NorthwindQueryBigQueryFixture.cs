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
}

public class NorthwindQueryBigQueryFixture : NorthwindQueryBigQueryFixture<BigQueryNorthwindModelCustomizer>
{
}