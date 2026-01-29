using Ivy.EntityFrameworkCore.BigQuery.Infrastructure;
using Ivy.EntityFrameworkCore.BigQuery.NetTopologySuite.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace Microsoft.EntityFrameworkCore;

/// <summary>
/// NetTopologySuite extension methods for <see cref="BigQueryDbContextOptionsBuilder"/>.
/// </summary>
public static class BigQueryNetTopologySuiteDbContextOptionsBuilderExtensions
{
    /// <summary>
    /// Use NetTopologySuite to access BigQuery spatial data (GEOGRAPHY type).
    /// </summary>
    /// <param name="optionsBuilder">The builder being used to configure BigQuery.</param>
    /// <returns>The options builder.</returns>
    public static BigQueryDbContextOptionsBuilder UseNetTopologySuite(
        this BigQueryDbContextOptionsBuilder optionsBuilder)
    {
        var coreOptionsBuilder = ((IRelationalDbContextOptionsBuilderInfrastructure)optionsBuilder).OptionsBuilder;

        var extension = coreOptionsBuilder.Options.FindExtension<BigQueryNetTopologySuiteOptionsExtension>()
            ?? new BigQueryNetTopologySuiteOptionsExtension();

        ((IDbContextOptionsBuilderInfrastructure)coreOptionsBuilder).AddOrUpdateExtension(extension);

        return optionsBuilder;
    }
}