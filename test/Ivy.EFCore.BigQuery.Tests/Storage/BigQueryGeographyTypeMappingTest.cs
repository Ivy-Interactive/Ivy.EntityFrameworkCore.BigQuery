using Ivy.EntityFrameworkCore.BigQuery.NetTopologySuite.Storage.Internal;
using Microsoft.EntityFrameworkCore.Storage;
using NetTopologySuite;
using NetTopologySuite.Geometries;
using Xunit;

#pragma warning disable EF1001 // Internal EF Core API usage.

namespace Ivy.EntityFrameworkCore.BigQuery.Tests.Storage;

/// <summary>
/// Tests for the BigQuery NTS geography type mapping plugin.
/// </summary>
public class BigQueryGeographyTypeMappingTest
{
    private readonly BigQueryNetTopologySuiteTypeMappingSourcePlugin _plugin;
    private readonly NtsGeometryServices _geometryServices = NtsGeometryServices.Instance;

    public BigQueryGeographyTypeMappingTest()
    {
        _plugin = new BigQueryNetTopologySuiteTypeMappingSourcePlugin(_geometryServices);
    }

    [Theory]
    [InlineData(typeof(Geometry))]
    [InlineData(typeof(Point))]
    [InlineData(typeof(LineString))]
    [InlineData(typeof(Polygon))]
    [InlineData(typeof(MultiPoint))]
    [InlineData(typeof(MultiLineString))]
    [InlineData(typeof(MultiPolygon))]
    [InlineData(typeof(GeometryCollection))]
    public void FindMapping_GeometryType_ReturnsGeographyMapping(Type geometryType)
    {
        var mappingInfo = new RelationalTypeMappingInfo(geometryType);
        var mapping = _plugin.FindMapping(mappingInfo);

        Assert.NotNull(mapping);
        Assert.Equal("GEOGRAPHY", mapping.StoreType);
        Assert.Equal(geometryType, mapping.ClrType);
        Assert.IsType<BigQueryGeographyTypeMapping>(mapping);
    }

    [Fact]
    public void FindMapping_StoreType_ReturnsGeographyMapping()
    {
        var mappingInfo = new RelationalTypeMappingInfo(storeTypeName: "GEOGRAPHY");
        var mapping = _plugin.FindMapping(mappingInfo);

        Assert.NotNull(mapping);
        Assert.Equal("GEOGRAPHY", mapping.StoreType);
        Assert.IsType<BigQueryGeographyTypeMapping>(mapping);
    }

    [Fact]
    public void FindMapping_NonGeometryType_ReturnsNull()
    {
        var mappingInfo = new RelationalTypeMappingInfo(typeof(string));
        var mapping = _plugin.FindMapping(mappingInfo);

        Assert.Null(mapping);
    }

    [Fact]
    public void FindMapping_NonGeographyStoreType_ReturnsNull()
    {
        var mappingInfo = new RelationalTypeMappingInfo(storeTypeName: "STRING");
        var mapping = _plugin.FindMapping(mappingInfo);

        Assert.Null(mapping);
    }

    [Fact]
    public void GenerateSqlLiteral_Point_GeneratesCorrectLiteral()
    {
        var mapping = new BigQueryGeographyTypeMapping(typeof(Point), _geometryServices);

        var point = new Point(-122.4194, 37.7749) { SRID = 4326 };
        var literal = mapping.GenerateSqlLiteral(point);

        Assert.Equal("ST_GEOGFROMTEXT('POINT (-122.4194 37.7749)')", literal);
    }

    [Fact]
    public void GenerateSqlLiteral_LineString_GeneratesCorrectLiteral()
    {
        var mapping = new BigQueryGeographyTypeMapping(typeof(LineString), _geometryServices);

        var lineString = new LineString([
            new Coordinate(0, 0),
            new Coordinate(1, 1),
            new Coordinate(2, 0)
        ]);
        var literal = mapping.GenerateSqlLiteral(lineString);

        Assert.Equal("ST_GEOGFROMTEXT('LINESTRING (0 0, 1 1, 2 0)')", literal);
    }

    [Fact]
    public void GenerateSqlLiteral_Polygon_GeneratesCorrectLiteral()
    {
        var mapping = new BigQueryGeographyTypeMapping(typeof(Polygon), _geometryServices);

        var polygon = new Polygon(new LinearRing([
            new Coordinate(0, 0),
            new Coordinate(4, 0),
            new Coordinate(4, 4),
            new Coordinate(0, 4),
            new Coordinate(0, 0)
        ]));
        var literal = mapping.GenerateSqlLiteral(polygon);

        Assert.Equal("ST_GEOGFROMTEXT('POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))')", literal);
    }

    [Fact]
    public void GenerateSqlLiteral_NullValue_GeneratesNullLiteral()
    {
        var mapping = new BigQueryGeographyTypeMapping(typeof(Point), _geometryServices);

        var literal = mapping.GenerateSqlLiteral(null);

        Assert.Equal("NULL", literal);
    }

    [Fact]
    public void Mapping_HasValueConverter()
    {
        var mapping = new BigQueryGeographyTypeMapping(typeof(Point), _geometryServices);

        var converter = mapping.Converter;
        Assert.NotNull(converter);
    }

    [Fact]
    public void ValueConverter_ConvertsPointToWkt()
    {
        var mapping = new BigQueryGeographyTypeMapping(typeof(Point), _geometryServices);
        var point = new Point(-122.4194, 37.7749) { SRID = 4326 };
        var converter = mapping.Converter;

        var wkt = converter?.ConvertToProvider(point);

        Assert.NotNull(wkt);
        Assert.IsType<string>(wkt);
        Assert.Contains("POINT", (string)wkt);
    }

    [Fact]
    public void ValueConverter_ConvertsWktToPoint()
    {
        var mapping = new BigQueryGeographyTypeMapping(typeof(Point), _geometryServices);
        var converter = mapping.Converter;
        var wkt = "POINT (-122.4194 37.7749)";

        var point = converter?.ConvertFromProvider(wkt);

        Assert.NotNull(point);
        Assert.IsType<Point>(point);
        var p = (Point)point;
        Assert.Equal(-122.4194, p.X, 4);
        Assert.Equal(37.7749, p.Y, 4);
    }
}
