using System.Collections;
using Google.Cloud.BigQuery.V2;
using NetTopologySuite;
using NetTopologySuite.Geometries;
using NetTopologySuite.IO;

namespace Ivy.Data.BigQuery.NetTopologySuite.Internal;

/// <summary>
/// NetTopologySuite implementation of <see cref="IGeographyTypeHandler"/> that provides
/// conversion between NTS Geometry types and BigQuery GEOGRAPHY values.
/// </summary>
public class NtsGeographyTypeHandler : IGeographyTypeHandler
{
    private readonly NtsGeometryServices _geometryServices;

    /// <summary>
    /// Creates a new instance of the NTS geography type handler.
    /// </summary>
    /// <param name="geometryServices">Optional geometry services instance. If null, uses the default instance.</param>
    public NtsGeographyTypeHandler(NtsGeometryServices? geometryServices = null)
    {
        _geometryServices = geometryServices ?? NtsGeometryServices.Instance;
    }

    /// <inheritdoc />
    public bool IsGeographyType(Type type)
    {
        return typeof(Geometry).IsAssignableFrom(type);
    }

    /// <inheritdoc />
    public bool TryConvertToGeography(object? value, out object? convertedValue)
    {
        if (value is Geometry geometry)
        {
            var wktWriter = new WKTWriter();
            convertedValue = wktWriter.Write(geometry);
            return true;
        }

        convertedValue = null;
        return false;
    }

    /// <inheritdoc />
    public bool TryConvertFromGeography(object? value, Type targetType, out object? result)
    {
        if (!typeof(Geometry).IsAssignableFrom(targetType))
        {
            result = null;
            return false;
        }

        Geometry? geometry = null;

        if (value is BigQueryGeography bqGeography)
        {
            // BigQueryGeography contains WKT in Text property
            var wktReader = new WKTReader(_geometryServices);
            geometry = wktReader.Read(bqGeography.Text);
        }
        else if (value is string stringValue)
        {
            // Direct WKT string
            var wktReader = new WKTReader(_geometryServices);
            geometry = wktReader.Read(stringValue);
        }
        else if (value is IDictionary<string, object> dict)
        {
            // BigQuery's custom format:
            // Simple: {"Text":"POINT(0 0)","SRID":4326}
            // Collection: {"Geometries":[{"Text":"LINESTRING(...)"},...],"SRID":4326}
            geometry = ParseBigQueryGeography(dict);
        }

        result = geometry;
        return geometry != null;
    }

    /// <summary>
    /// Parses BigQuery's custom geography dictionary format into NTS Geometry.
    /// </summary>
    private Geometry? ParseBigQueryGeography(IDictionary<string, object> dict)
    {
        var wktReader = new WKTReader(_geometryServices);

        if (dict.TryGetValue("Text", out var textObj) && textObj is string wkt)
        {
            var geometry = wktReader.Read(wkt);

            if (dict.TryGetValue("SRID", out var sridObj))
            {
                var srid = Convert.ToInt32(sridObj);
                geometry.SRID = srid;
            }

            return geometry;
        }

        if (dict.TryGetValue("Geometries", out var geometriesObj))
        {
            IEnumerable<object>? geometriesEnumerable = null;

            if (geometriesObj is object[] arr)
                geometriesEnumerable = arr;
            else if (geometriesObj is IList list)
                geometriesEnumerable = list.Cast<object>();
            else if (geometriesObj is IEnumerable enumerable && geometriesObj is not string)
                geometriesEnumerable = enumerable.Cast<object>();

            if (geometriesEnumerable == null)
            {
                throw new InvalidCastException($"Geometries property has unexpected type: {geometriesObj?.GetType().FullName}");
            }

            var geometriesArray = geometriesEnumerable.ToArray();
            var geometries = new List<Geometry>();

            foreach (var item in geometriesArray)
            {
                if (item is IDictionary<string, object> geomDict)
                {
                    var geom = ParseBigQueryGeography(geomDict);
                    if (geom != null)
                    {
                        geometries.Add(geom);
                    }
                }
                else if (item is BigQueryGeography bqGeom)
                {
                    var geom = wktReader.Read(bqGeom.Text);
                    geometries.Add(geom);
                }
                else
                {
                    throw new InvalidCastException($"Geometry item has unexpected type: {item?.GetType().FullName}. Value: {item}");
                }
            }

            if (geometries.Count == 0)
            {
                throw new InvalidCastException($"No geometries parsed from array of {geometriesArray.Length} items");
            }

            var srid = 0;
            if (dict.TryGetValue("SRID", out var collectionSridObj))
            {
                srid = Convert.ToInt32(collectionSridObj);
            }

            var factory = _geometryServices.CreateGeometryFactory(srid);

            if (geometries.All(g => g is Point))
            {
                return factory.CreateMultiPoint(geometries.Cast<Point>().ToArray());
            }
            if (geometries.All(g => g is LineString))
            {
                return factory.CreateMultiLineString(geometries.Cast<LineString>().ToArray());
            }
            if (geometries.All(g => g is Polygon))
            {
                return factory.CreateMultiPolygon(geometries.Cast<Polygon>().ToArray());
            }

            // Mixed types
            return factory.CreateGeometryCollection(geometries.ToArray());
        }

        return null;
    }
}
