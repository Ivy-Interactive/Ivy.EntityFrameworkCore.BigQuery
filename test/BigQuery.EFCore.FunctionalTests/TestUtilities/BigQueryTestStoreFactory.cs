using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Ivy.EntityFrameworkCore.BigQuery.Extensions;

namespace Ivy.EntityFrameworkCore.BigQuery.FunctionalTests.TestUtilities
{
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
}
