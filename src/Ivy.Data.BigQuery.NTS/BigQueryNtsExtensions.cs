using Ivy.Data.BigQuery.NetTopologySuite.Internal;
using NetTopologySuite;
using NetTopologySuite.Geometries;

namespace Ivy.Data.BigQuery;

/// <summary>
/// Extension methods for adding NetTopologySuite support to the BigQuery ADO.NET provider.
/// </summary>
public static class BigQueryNtsExtensions
{
    /// <summary>
    /// Enables NetTopologySuite support for the BigQuery ADO.NET provider.
    /// This allows using NTS Geometry types with BigQuery GEOGRAPHY columns.
    /// </summary>
    /// <param name="geometryServices">
    /// Optional geometry services instance to use for creating geometry objects.
    /// If null, uses <see cref="NtsGeometryServices.Instance"/>.
    /// </param>
    /// <remarks>
    /// This method registers the NTS type handler globally. Call it once during application startup.
    ///
    /// Example usage:
    /// <code>
    /// BigQueryNtsExtensions.UseNetTopologySuite();
    ///
    /// // Now you can use NTS types with BigQuery
    /// using var connection = new BigQueryConnection(connectionString);
    /// var cmd = connection.CreateCommand();
    /// cmd.CommandText = "SELECT location FROM places WHERE ST_CONTAINS(@region, location)";
    /// cmd.Parameters.Add(new BigQueryParameter("@region", myPolygon));
    /// </code>
    /// </remarks>
    public static void UseNetTopologySuite(NtsGeometryServices? geometryServices = null)
    {
        BigQueryTypeHandlers.UseGeographyHandler(new NtsGeographyTypeHandler(geometryServices));
    }
}
