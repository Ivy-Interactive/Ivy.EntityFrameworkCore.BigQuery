using Microsoft.EntityFrameworkCore.Storage;
using NetTopologySuite;
using NetTopologySuite.Geometries;

namespace Ivy.EntityFrameworkCore.BigQuery.NetTopologySuite.Storage.Internal;

/// <summary>
/// Type mapping source plugin for NetTopologySuite geometry types to BigQuery GEOGRAPHY.
/// </summary>
public class BigQueryNetTopologySuiteTypeMappingSourcePlugin : IRelationalTypeMappingSourcePlugin
{
    private readonly NtsGeometryServices _geometryServices;

    /// <summary>
    /// Creates a new instance.
    /// </summary>
    public BigQueryNetTopologySuiteTypeMappingSourcePlugin(NtsGeometryServices geometryServices)
    {
        _geometryServices = geometryServices;
    }

    /// <inheritdoc />
    public virtual RelationalTypeMapping? FindMapping(in RelationalTypeMappingInfo mappingInfo)
    {
        var clrType = mappingInfo.ClrType;
        var storeTypeName = mappingInfo.StoreTypeName;

        if (clrType != null && typeof(Geometry).IsAssignableFrom(clrType))
        {
            return new BigQueryGeographyTypeMapping(clrType, _geometryServices);
        }

        if (storeTypeName != null &&
            storeTypeName.Equals("GEOGRAPHY", StringComparison.OrdinalIgnoreCase))
        {
            var targetType = clrType ?? typeof(Geometry);
            if (typeof(Geometry).IsAssignableFrom(targetType))
            {
                return new BigQueryGeographyTypeMapping(targetType, _geometryServices);
            }
        }

        return null;
    }
}