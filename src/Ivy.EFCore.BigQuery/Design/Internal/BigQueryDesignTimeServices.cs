using Ivy.EntityFrameworkCore.BigQuery.Extensions;
using Ivy.EntityFrameworkCore.BigQuery.Scaffolding.Internal;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore.Scaffolding;
using Microsoft.Extensions.DependencyInjection;

[assembly: DesignTimeProviderServices("Ivy.EntityFrameworkCore.BigQuery.Design.Internal.BigQueryDesignTimeServices")]

namespace Ivy.EntityFrameworkCore.BigQuery.Design.Internal
{
    public class BigQueryDesignTimeServices : IDesignTimeServices
    {
        public void ConfigureDesignTimeServices(IServiceCollection serviceCollection)
        {
            serviceCollection.AddEntityFrameworkBigQuery();

            new EntityFrameworkRelationalDesignServicesBuilder(serviceCollection)
                .TryAdd<IDatabaseModelFactory, BigQueryDatabaseModelFactory>()
                .TryAdd<IProviderConfigurationCodeGenerator, BigQueryCodeGenerator>()
                .TryAdd<IAnnotationCodeGenerator, BigQueryAnnotationCodeGenerator>()
                .TryAddProviderSpecificServices(
                    services => services
                        .TryAddSingleton<IModelCodeGeneratorSelector, BigQueryModelCodeGeneratorSelector>()
                        .TryAddSingletonEnumerable<IModelCodeGenerator, BigQueryModelCodeGenerator>())
                .TryAddCoreServices();
        }
    }
}
