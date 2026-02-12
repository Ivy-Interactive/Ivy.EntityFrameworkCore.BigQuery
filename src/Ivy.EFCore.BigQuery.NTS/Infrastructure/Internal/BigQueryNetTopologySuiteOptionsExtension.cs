using Ivy.EntityFrameworkCore.BigQuery.NetTopologySuite.Storage.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;

namespace Ivy.EntityFrameworkCore.BigQuery.NetTopologySuite.Infrastructure.Internal;

/// <summary>
/// Options extension for enabling NetTopologySuite spatial support in BigQuery.
/// </summary>
public class BigQueryNetTopologySuiteOptionsExtension : IDbContextOptionsExtension
{
    private DbContextOptionsExtensionInfo? _info;

    /// <summary>
    /// Creates a new instance.
    /// </summary>
    public BigQueryNetTopologySuiteOptionsExtension()
    {
    }

    /// <summary>
    /// Creates a new instance from an existing extension (for cloning).
    /// </summary>
    protected BigQueryNetTopologySuiteOptionsExtension(BigQueryNetTopologySuiteOptionsExtension copyFrom)
    {
    }

    /// <inheritdoc />
    public virtual DbContextOptionsExtensionInfo Info
        => _info ??= new ExtensionInfo(this);

    /// <inheritdoc />
    public virtual void ApplyServices(IServiceCollection services)
        => services.AddEntityFrameworkBigQueryNetTopologySuite();

    /// <inheritdoc />
    public virtual void Validate(IDbContextOptions options)
    {
        var internalServiceProvider = options.FindExtension<CoreOptionsExtension>()?.InternalServiceProvider;
        if (internalServiceProvider != null)
        {
            using var scope = internalServiceProvider.CreateScope();
            var plugins = scope.ServiceProvider.GetService<IEnumerable<IRelationalTypeMappingSourcePlugin>>();
            if (plugins?.Any(s => s is BigQueryNetTopologySuiteTypeMappingSourcePlugin) != true)
            {
                throw new InvalidOperationException(
                    "NetTopologySuite spatial support requires AddEntityFrameworkBigQueryNetTopologySuite() " +
                    "to be called on the service collection.");
            }
        }
    }

    /// <summary>
    /// Creates a clone of this extension.
    /// </summary>
    protected virtual BigQueryNetTopologySuiteOptionsExtension Clone()
        => new(this);

    private sealed class ExtensionInfo : DbContextOptionsExtensionInfo
    {
        public ExtensionInfo(IDbContextOptionsExtension extension)
            : base(extension)
        {
        }

        private new BigQueryNetTopologySuiteOptionsExtension Extension
            => (BigQueryNetTopologySuiteOptionsExtension)base.Extension;

        public override bool IsDatabaseProvider => false;

        public override string LogFragment => "using NetTopologySuite ";

        public override int GetServiceProviderHashCode() => 0;

        public override bool ShouldUseSameServiceProvider(DbContextOptionsExtensionInfo other)
            => other is ExtensionInfo;

        public override void PopulateDebugInfo(IDictionary<string, string> debugInfo)
            => debugInfo["BigQuery:NetTopologySuite"] = "1";
    }
}