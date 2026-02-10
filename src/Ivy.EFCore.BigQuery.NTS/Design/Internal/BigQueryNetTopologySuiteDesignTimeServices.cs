using Ivy.EntityFrameworkCore.BigQuery.NetTopologySuite.Scaffolding.Internal;
using Ivy.EntityFrameworkCore.BigQuery.NetTopologySuite.Storage.Internal;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore.Scaffolding;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using NetTopologySuite;

namespace Ivy.EntityFrameworkCore.BigQuery.NetTopologySuite.Design.Internal;

/// <summary>
/// Design-time services for NetTopologySuite spatial support in BigQuery scaffolding.
/// </summary>
public class BigQueryNetTopologySuiteDesignTimeServices : IDesignTimeServices
{
    /// <inheritdoc />
    public virtual void ConfigureDesignTimeServices(IServiceCollection serviceCollection)
        => serviceCollection
            .AddSingleton(NtsGeometryServices.Instance)
            .AddSingleton<IRelationalTypeMappingSourcePlugin, BigQueryNetTopologySuiteTypeMappingSourcePlugin>()
            .AddSingleton<IProviderCodeGeneratorPlugin, BigQueryNetTopologySuiteCodeGeneratorPlugin>();
}