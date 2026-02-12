using Ivy.EntityFrameworkCore.BigQuery.NetTopologySuite.Infrastructure.Internal;
using Ivy.EntityFrameworkCore.BigQuery.NetTopologySuite.Query.Internal;
using Ivy.EntityFrameworkCore.BigQuery.NetTopologySuite.Storage.Internal;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using NetTopologySuite;

namespace Microsoft.EntityFrameworkCore;

/// <summary>
/// NetTopologySuite extension methods for <see cref="IServiceCollection"/>.
/// </summary>
public static class BigQueryNetTopologySuiteServiceCollectionExtensions
{
    /// <summary>
    /// Adds the services required for NetTopologySuite support in the BigQuery provider for Entity Framework Core.
    /// </summary>
    /// <param name="serviceCollection">The <see cref="IServiceCollection"/> to add services to.</param>
    /// <returns>The same service collection so that multiple calls can be chained.</returns>
    public static IServiceCollection AddEntityFrameworkBigQueryNetTopologySuite(
        this IServiceCollection serviceCollection)
    {
        serviceCollection.TryAddSingleton(_ => NtsGeometryServices.Instance);

        new EntityFrameworkRelationalServicesBuilder(serviceCollection)
            .TryAdd<IRelationalTypeMappingSourcePlugin, BigQueryNetTopologySuiteTypeMappingSourcePlugin>()
            .TryAdd<IMethodCallTranslatorPlugin, BigQueryNetTopologySuiteMethodCallTranslatorPlugin>()
            .TryAdd<IMemberTranslatorPlugin, BigQueryNetTopologySuiteMemberTranslatorPlugin>();

        return serviceCollection;
    }
}