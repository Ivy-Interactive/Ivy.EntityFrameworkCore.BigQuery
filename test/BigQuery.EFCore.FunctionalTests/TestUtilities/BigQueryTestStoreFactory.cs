using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.Extensions.DependencyInjection;
using Ivy.EntityFrameworkCore.BigQuery.Extensions;

namespace Ivy.EntityFrameworkCore.BigQuery.TestUtilities;

public class BigQueryTestStoreFactory : RelationalTestStoreFactory
{
    protected BigQueryTestStoreFactory()
    {            
    }

    public static BigQueryTestStoreFactory Instance { get; } = new();

    public override TestStore Create(string storeName)
        => new BigQueryTestStore(storeName, shared: false);

    public override TestStore GetOrCreate(string storeName)
        => new BigQueryTestStore(storeName, shared: true);

    public override IServiceCollection AddProviderServices(IServiceCollection serviceCollection)
        => serviceCollection.AddEntityFrameworkBigQuery();
}
