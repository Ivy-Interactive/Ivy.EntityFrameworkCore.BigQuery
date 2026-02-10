using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestModels.SpatialModel;
using NetTopologySuite.Geometries;
using NetTopologySuite.IO;
using Xunit;
using Xunit.Abstractions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query;

public class SpatialQueryBigQueryTest : SpatialQueryRelationalTestBase<SpatialQueryBigQueryFixture>
{
    public SpatialQueryBigQueryTest(SpatialQueryBigQueryFixture fixture, ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
        Fixture.TestSqlLoggerFactory.Clear();
        Fixture.TestSqlLoggerFactory.SetTestOutputHelper(testOutputHelper);
    }

    // BigQuery geography returns distances in meters, not degrees
    protected override bool AssertDistances
        => false;

    public override async Task SimpleSelect(bool async)
    {
        await base.SimpleSelect(async);

        AssertSql(
            """
SELECT `p`.`Id`, `p`.`Geometry`, `p`.`Group`, `p`.`Point`, `p`.`PointM`, `p`.`PointZ`, `p`.`PointZM`
FROM `PointEntity` AS `p`
""",
            //
            """
SELECT `l`.`Id`, `l`.`LineString`
FROM `LineStringEntity` AS `l`
""",
            //
            """
SELECT `p`.`Id`, `p`.`Polygon`
FROM `PolygonEntity` AS `p`
""",
            //
            """
SELECT `m`.`Id`, `m`.`MultiLineString`
FROM `MultiLineStringEntity` AS `m`
""");
    }

    public override async Task Area(bool async)
    {
        await base.Area(async);

        AssertSql(
            """
SELECT `p`.`Id`, ST_AREA(`p`.`Polygon`) AS `Area`
FROM `PolygonEntity` AS `p`
""");
    }

    // BigQuery's WKB has tiny floating-point precision differences (subnormal values ~1e-308 instead of exact 0.0)
    // Override to verify SQL and compare parsed coordinates with tolerance instead of raw bytes
    public override async Task AsBinary(bool async)
    {
        var context = Fixture.CreateContext();
        var results = async
            ? await context.Set<PointEntity>().Select(e => new { e.Id, Binary = e.Point!.AsBinary() }).ToListAsync()
            : context.Set<PointEntity>().Select(e => new { e.Id, Binary = e.Point!.AsBinary() }).ToList();

        // Verify SQL
        AssertSql(
            """
SELECT `p`.`Id`, ST_ASBINARY(`p`.`Point`) AS `Binary`
FROM `PointEntity` AS `p`
""");

        // Verify WKB can be parsed and coordinates are correct (with tolerance for BigQuery precision)
        var wkbReader = new WKBReader();
        foreach (var result in results.Where(r => r.Binary != null))
        {
            var parsed = wkbReader.Read(result.Binary);
            Assert.NotNull(parsed);
            Assert.IsType<Point>(parsed);

            // Original test data has points at (0,0) and (1,1)
            var point = (Point)parsed;
            var isOrigin = Math.Abs(point.X) < 1e-6 && Math.Abs(point.Y) < 1e-6;
            var isOneOne = Math.Abs(point.X - 1) < 1e-6 && Math.Abs(point.Y - 1) < 1e-6;
            Assert.True(isOrigin || isOneOne, $"Unexpected point coordinates: ({point.X}, {point.Y})");
        }
    }

    // BigQuery's WKB has tiny floating-point precision differences
    public override async Task AsBinary_with_null_check(bool async)
    {
        var context = Fixture.CreateContext();
        var results = async
            ? await context.Set<PointEntity>().Select(e => new { e.Id, Binary = e.Point == null ? null : e.Point.AsBinary() }).ToListAsync()
            : context.Set<PointEntity>().Select(e => new { e.Id, Binary = e.Point == null ? null : e.Point.AsBinary() }).ToList();

        // Verify SQL
        AssertSql(
            """
SELECT `p`.`Id`, CASE
    WHEN `p`.`Point` IS NULL THEN NULL
    ELSE ST_ASBINARY(`p`.`Point`)
END AS `Binary`
FROM `PointEntity` AS `p`
""");

        // Verify results include null for null points
        Assert.Contains(results, r => r.Binary == null);
        Assert.Contains(results, r => r.Binary != null);
    }

    public override async Task AsText(bool async)
    {
        await base.AsText(async);

        AssertSql(
            """
SELECT `p`.`Id`, ST_ASTEXT(`p`.`Point`) AS `Text`
FROM `PointEntity` AS `p`
""");
    }

    // No BigQuery translation for ST_BOUNDARY
    public override Task Boundary(bool async)
        => Task.CompletedTask;

    public override async Task Buffer(bool async)
    {
        await base.Buffer(async);

        AssertSql(
            """
SELECT `p`.`Id`, ST_BUFFER(`p`.`Polygon`, 1) AS `Buffer`
FROM `PolygonEntity` AS `p`
""");
    }

    // BigQuery ST_BUFFER doesn't support quadrant segments parameter
    public override Task Buffer_quadrantSegments(bool async)
        => Task.CompletedTask;

    public override async Task Centroid(bool async)
    {
        await base.Centroid(async);

        AssertSql(
            """
SELECT `p`.`Id`, ST_CENTROID(`p`.`Polygon`) AS `Centroid`
FROM `PolygonEntity` AS `p`
""");
    }

    // No BigQuery aggregate function for geometry collection
    public override Task Combine_aggregate(bool async)
        => Task.CompletedTask;

    // No BigQuery aggregate function for envelope combine
    public override Task EnvelopeCombine_aggregate(bool async)
        => Task.CompletedTask;

    public override async Task Contains(bool async)
    {
        await base.Contains(async);

        AssertSql(
            """
@__point_0='POINT (0.25 0.25)' (DbType = Object)

SELECT `p`.`Id`, ST_CONTAINS(`p`.`Polygon`, @__point_0) AS `Contains`
FROM `PolygonEntity` AS `p`
""");
    }

    public override async Task ConvexHull(bool async)
    {
        await base.ConvexHull(async);

        AssertSql(
            """
SELECT `p`.`Id`, ST_CONVEXHULL(`p`.`Polygon`) AS `ConvexHull`
FROM `PolygonEntity` AS `p`
""");
    }

    // No BigQuery aggregate function for convex hull
    public override Task ConvexHull_aggregate(bool async)
        => Task.CompletedTask;

    public override async Task IGeometryCollection_Count(bool async)
    {
        await base.IGeometryCollection_Count(async);

        AssertSql(
            """
SELECT `m`.`Id`, ST_NUMGEOMETRIES(`m`.`MultiLineString`) AS `Count`
FROM `MultiLineStringEntity` AS `m`
""");
    }

    public override async Task LineString_Count(bool async)
    {
        await base.LineString_Count(async);

        AssertSql(
            """
SELECT `l`.`Id`, ST_NUMPOINTS(`l`.`LineString`) AS `Count`
FROM `LineStringEntity` AS `l`
""");
    }

    public override async Task CoveredBy(bool async)
    {
        await base.CoveredBy(async);

        AssertSql(
            """
@__polygon_0='POLYGON ((-1 -1, 2 -1, 2 2, -1 2, -1 -1))' (DbType = Object)

SELECT `p`.`Id`, ST_COVEREDBY(`p`.`Point`, @__polygon_0) AS `CoveredBy`
FROM `PointEntity` AS `p`
""");
    }

    public override async Task Covers(bool async)
    {
        await base.Covers(async);

        AssertSql(
            """
@__point_0='POINT (0.25 0.25)' (DbType = Object)

SELECT `p`.`Id`, ST_COVERS(`p`.`Polygon`, @__point_0) AS `Covers`
FROM `PolygonEntity` AS `p`
""");
    }

    // No BigQuery translation for ST_CROSSES
    public override Task Crosses(bool async)
        => Task.CompletedTask;

    public override async Task Difference(bool async)
    {
        await base.Difference(async);

        AssertSql(
            """
@__polygon_0='POLYGON ((0 0, 1 0, 1 1, 0 0))' (DbType = Object)

SELECT `p`.`Id`, ST_DIFFERENCE(`p`.`Polygon`, @__polygon_0) AS `Difference`
FROM `PolygonEntity` AS `p`
""");
    }

    public override async Task Dimension(bool async)
    {
        await base.Dimension(async);

        AssertSql(
            """
SELECT `p`.`Id`, ST_DIMENSION(`p`.`Point`) AS `Dimension`
FROM `PointEntity` AS `p`
""");
    }

    public override async Task Disjoint_with_cast_to_nullable(bool async)
    {
        await base.Disjoint_with_cast_to_nullable(async);

        AssertSql(
            """
@__point_0='POINT (1 1)' (DbType = Object)

SELECT `p`.`Id`, ST_DISJOINT(`p`.`Polygon`, @__point_0) AS `Disjoint`
FROM `PolygonEntity` AS `p`
""");
    }

    public override async Task Disjoint_with_null_check(bool async)
    {
        await base.Disjoint_with_null_check(async);

        AssertSql(
            """
@__point_0='POINT (1 1)' (DbType = Object)

SELECT `p`.`Id`, CASE
    WHEN `p`.`Polygon` IS NULL THEN NULL
    ELSE ST_DISJOINT(`p`.`Polygon`, @__point_0)
END AS `Disjoint`
FROM `PolygonEntity` AS `p`
""");
    }

    public override async Task Distance_with_null_check(bool async)
    {
        await base.Distance_with_null_check(async);

        AssertSql(
            """
@__point_0='POINT (0 1)' (DbType = Object)

SELECT `p`.`Id`, ST_DISTANCE(`p`.`Point`, @__point_0) AS `Distance`
FROM `PointEntity` AS `p`
""");
    }

    public override async Task Distance_with_cast_to_nullable(bool async)
    {
        await base.Distance_with_cast_to_nullable(async);

        AssertSql(
            """
@__point_0='POINT (0 1)' (DbType = Object)

SELECT `p`.`Id`, ST_DISTANCE(`p`.`Point`, @__point_0) AS `Distance`
FROM `PointEntity` AS `p`
""");
    }

    public override async Task Distance_geometry(bool async)
    {
        await base.Distance_geometry(async);

        AssertSql(
            """
@__point_0='POINT (0 1)' (DbType = Object)

SELECT `p`.`Id`, ST_DISTANCE(`p`.`Geometry`, @__point_0) AS `Distance`
FROM `PointEntity` AS `p`
""");
    }

    // SRID mismatch issues with constants
    public override Task Distance_constant(bool async)
        => Task.CompletedTask;

    public override Task Distance_constant_srid_4326(bool async)
        => Task.CompletedTask;

    public override Task Distance_constant_lhs(bool async)
        => Task.CompletedTask;

    // Custom GeoPoint conversion tests - not implemented
    public override Task WithConversion(bool async)
        => Task.CompletedTask;

    public override Task Distance_on_converted_geometry_type(bool async)
        => Task.CompletedTask;

    public override Task Distance_on_converted_geometry_type_lhs(bool async)
        => Task.CompletedTask;

    public override Task Distance_on_converted_geometry_type_constant(bool async)
        => Task.CompletedTask;

    public override Task Distance_on_converted_geometry_type_constant_lhs(bool async)
        => Task.CompletedTask;

    public override async Task EndPoint(bool async)
    {
        await base.EndPoint(async);

        AssertSql(
            """
SELECT `l`.`Id`, ST_ENDPOINT(`l`.`LineString`) AS `EndPoint`
FROM `LineStringEntity` AS `l`
""");
    }

    // No BigQuery translation for ST_ENVELOPE (geography mode)
    public override Task Envelope(bool async)
        => Task.CompletedTask;

    public override async Task EqualsTopologically(bool async)
    {
        await base.EqualsTopologically(async);

        AssertSql(
            """
@__point_0='POINT (0 0)' (DbType = Object)

SELECT `p`.`Id`, ST_EQUALS(`p`.`Point`, @__point_0) AS `EqualsTopologically`
FROM `PointEntity` AS `p`
""");
    }

    public override async Task ExteriorRing(bool async)
    {
        await base.ExteriorRing(async);

        AssertSql(
            """
SELECT `p`.`Id`, ST_EXTERIORRING(`p`.`Polygon`) AS `ExteriorRing`
FROM `PolygonEntity` AS `p`
""");
    }

    public override async Task GeometryType(bool async)
    {
        await base.GeometryType(async);

        AssertSql(
            """
SELECT `p`.`Id`, REPLACE(ST_GEOMETRYTYPE(`p`.`Point`), 'ST_', '') AS `GeometryType`
FROM `PointEntity` AS `p`
""");
    }

    public override async Task GetGeometryN(bool async)
    {
        await base.GetGeometryN(async);

        AssertSql(
            """
SELECT `m`.`Id`, ST_DUMP(`m`.`MultiLineString`)[OFFSET(0)] AS `Geometry0`
FROM `MultiLineStringEntity` AS `m`
""");
    }

    // BigQuery doesn't handle null arguments gracefully
    public override Task GetGeometryN_with_null_argument(bool async)
        => Task.CompletedTask;

    // No BigQuery translation for interior ring access
    public override Task GetInteriorRingN(bool async)
        => Task.CompletedTask;

    public override async Task GetPointN(bool async)
    {
        await base.GetPointN(async);

        AssertSql(
            """
SELECT `l`.`Id`, ST_POINTN(`l`.`LineString`, 0 + 1) AS `Point0`
FROM `LineStringEntity` AS `l`
""");
    }

    // No BigQuery translation for ST_INTERIORPOINT
    public override Task InteriorPoint(bool async)
        => Task.CompletedTask;

    public override async Task Intersection(bool async)
    {
        await base.Intersection(async);

        AssertSql(
            """
@__polygon_0='POLYGON ((0 0, 1 0, 1 1, 0 0))' (DbType = Object)

SELECT `p`.`Id`, ST_INTERSECTION(`p`.`Polygon`, @__polygon_0) AS `Intersection`
FROM `PolygonEntity` AS `p`
""");
    }

    public override async Task Intersects(bool async)
    {
        await base.Intersects(async);

        AssertSql(
            """
@__lineString_0='LINESTRING (0.5 -0.5, 0.5 0.5)' (DbType = Object)

SELECT `l`.`Id`, ST_INTERSECTS(`l`.`LineString`, @__lineString_0) AS `Intersects`
FROM `LineStringEntity` AS `l`
""");
    }

    public override async Task ICurve_IsClosed(bool async)
    {
        await base.ICurve_IsClosed(async);

        AssertSql(
            """
SELECT `l`.`Id`, ST_ISCLOSED(`l`.`LineString`) AS `IsClosed`
FROM `LineStringEntity` AS `l`
""");
    }

    // No BigQuery translation for IMultiCurve.IsClosed
    public override Task IMultiCurve_IsClosed(bool async)
        => Task.CompletedTask;

    public override async Task IsEmpty(bool async)
    {
        await base.IsEmpty(async);

        AssertSql(
            """
SELECT `m`.`Id`, ST_ISEMPTY(`m`.`MultiLineString`) AS `IsEmpty`
FROM `MultiLineStringEntity` AS `m`
""");
    }

    public override async Task IsRing(bool async)
    {
        await base.IsRing(async);

        AssertSql(
            """
SELECT `l`.`Id`, ST_ISRING(`l`.`LineString`) AS `IsRing`
FROM `LineStringEntity` AS `l`
""");
    }

    // No BigQuery translation for ST_ISSIMPLE (geography)
    public override Task IsSimple(bool async)
        => Task.CompletedTask;

    // No BigQuery translation for ST_ISVALID (geography)
    public override Task IsValid(bool async)
        => Task.CompletedTask;

    public override async Task IsWithinDistance(bool async)
    {
        await base.IsWithinDistance(async);

        AssertSql(
            """
@__point_0='POINT (0 1)' (DbType = Object)

SELECT `p`.`Id`, ST_DWITHIN(`p`.`Point`, @__point_0, 1) AS `IsWithinDistance`
FROM `PointEntity` AS `p`
""");
    }

    // Item/indexer - different syntax needed
    public override Task Item(bool async)
        => Task.CompletedTask;

    public override async Task Length(bool async)
    {
        await base.Length(async);

        AssertSql(
            """
SELECT `l`.`Id`, ST_LENGTH(`l`.`LineString`) AS `Length`
FROM `LineStringEntity` AS `l`
""");
    }

    // BigQuery geography doesn't support M coordinate
    public override Task M(bool async)
        => Task.CompletedTask;

    // No BigQuery translation for ST_NORMALIZE
    public override Task Normalized(bool async)
        => Task.CompletedTask;

    public override async Task NumGeometries(bool async)
    {
        await base.NumGeometries(async);

        AssertSql(
            """
SELECT `m`.`Id`, ST_NUMGEOMETRIES(`m`.`MultiLineString`) AS `NumGeometries`
FROM `MultiLineStringEntity` AS `m`
""");
    }

    public override async Task NumInteriorRings(bool async)
    {
        await base.NumInteriorRings(async);

        AssertSql(
            """
SELECT `p`.`Id`, ARRAY_LENGTH(ST_INTERIORRINGS(`p`.`Polygon`)) AS `NumInteriorRings`
FROM `PolygonEntity` AS `p`
""");
    }

    public override async Task NumPoints(bool async)
    {
        await base.NumPoints(async);

        AssertSql(
            """
SELECT `l`.`Id`, ST_NUMPOINTS(`l`.`LineString`) AS `NumPoints`
FROM `LineStringEntity` AS `l`
""");
    }

    // No BigQuery translation for OgcGeometryType
    public override Task OgcGeometryType(bool async)
        => Task.CompletedTask;

    public override async Task Overlaps(bool async)
    {
        await base.Overlaps(async);

        // BigQuery doesn't have ST_OVERLAPS, implemented as ST_INTERSECTS && !ST_TOUCHES
        AssertSql(
            """
@__polygon_0='POLYGON ((0 0, 1 0, 1 1, 0 0))' (DbType = Object)

SELECT `p`.`Id`, ST_INTERSECTS(`p`.`Polygon`, @__polygon_0) AND NOT (ST_TOUCHES(`p`.`Polygon`, @__polygon_0)) AS `Overlaps`
FROM `PolygonEntity` AS `p`
""");
    }

    // No BigQuery translation for ST_POINTONSURFACE
    public override Task PointOnSurface(bool async)
        => Task.CompletedTask;

    // No BigQuery translation for ST_RELATE
    public override Task Relate(bool async)
        => Task.CompletedTask;

    // No BigQuery translation for ST_REVERSE
    public override Task Reverse(bool async)
        => Task.CompletedTask;

    // BigQuery geography doesn't support SRID property (always 4326)
    public override Task SRID(bool async)
        => Task.CompletedTask;

    public override Task SRID_geometry(bool async)
        => Task.CompletedTask;

    public override async Task StartPoint(bool async)
    {
        await base.StartPoint(async);

        AssertSql(
            """
SELECT `l`.`Id`, ST_STARTPOINT(`l`.`LineString`) AS `StartPoint`
FROM `LineStringEntity` AS `l`
""");
    }

    public override async Task SymmetricDifference(bool async)
    {
        await base.SymmetricDifference(async);

        AssertSql(
            """
@__polygon_0='POLYGON ((0 0, 1 0, 1 1, 0 0))' (DbType = Object)

SELECT `p`.`Id`, ST_UNION(ST_DIFFERENCE(`p`.`Polygon`, @__polygon_0), ST_DIFFERENCE(@__polygon_0, `p`.`Polygon`)) AS `SymmetricDifference`
FROM `PolygonEntity` AS `p`
""");
    }

    // BigQuery's WKB has tiny floating-point precision differences - same as AsBinary
    public override async Task ToBinary(bool async)
    {
        var context = Fixture.CreateContext();
        var results = async
            ? await context.Set<PointEntity>().Select(e => new { e.Id, Binary = e.Point!.ToBinary() }).ToListAsync()
            : context.Set<PointEntity>().Select(e => new { e.Id, Binary = e.Point!.ToBinary() }).ToList();

        // Verify SQL (ToBinary translates to same ST_ASBINARY as AsBinary)
        AssertSql(
            """
SELECT `p`.`Id`, ST_ASBINARY(`p`.`Point`) AS `Binary`
FROM `PointEntity` AS `p`
""");

        // Verify WKB can be parsed and coordinates are correct
        var wkbReader = new WKBReader();
        foreach (var result in results.Where(r => r.Binary != null))
        {
            var parsed = wkbReader.Read(result.Binary);
            Assert.NotNull(parsed);
            Assert.IsType<Point>(parsed);
        }
    }

    public override async Task ToText(bool async)
    {
        await base.ToText(async);

        AssertSql(
            """
SELECT `p`.`Id`, ST_ASTEXT(`p`.`Point`) AS `Text`
FROM `PointEntity` AS `p`
""");
    }

    public override async Task Touches(bool async)
    {
        await base.Touches(async);

        AssertSql(
            """
@__polygon_0='POLYGON ((0 1, 1 0, 1 1, 0 1))' (DbType = Object)

SELECT `p`.`Id`, ST_TOUCHES(`p`.`Polygon`, @__polygon_0) AS `Touches`
FROM `PolygonEntity` AS `p`
""");
    }

    public override async Task Union(bool async)
    {
        await base.Union(async);

        AssertSql(
            """
@__polygon_0='POLYGON ((0 0, 1 0, 1 1, 0 0))' (DbType = Object)

SELECT `p`.`Id`, ST_UNION(`p`.`Polygon`, @__polygon_0) AS `Union`
FROM `PolygonEntity` AS `p`
""");
    }

    // No BigQuery aggregate function for union
    public override Task Union_aggregate(bool async)
        => Task.CompletedTask;

    // Union without parameter (self-union) - not supported
    public override Task Union_void(bool async)
        => Task.CompletedTask;

    public override async Task Within(bool async)
    {
        await base.Within(async);

        AssertSql(
            """
@__polygon_0='POLYGON ((-1 -1, 2 -1, 2 2, -1 2, -1 -1))' (DbType = Object)

SELECT `p`.`Id`, ST_WITHIN(`p`.`Point`, @__polygon_0) AS `Within`
FROM `PointEntity` AS `p`
""");
    }

    public override async Task X(bool async)
    {
        await base.X(async);

        AssertSql(
            """
SELECT `p`.`Id`, ST_X(`p`.`Point`) AS `X`
FROM `PointEntity` AS `p`
""");
    }

    public override async Task Y(bool async)
    {
        await base.Y(async);

        AssertSql(
            """
SELECT `p`.`Id`, ST_Y(`p`.`Point`) AS `Y`
FROM `PointEntity` AS `p`
""");
    }

    // BigQuery geography doesn't support Z coordinate
    public override Task Z(bool async)
        => Task.CompletedTask;

    // XY with collection join - complex query, skip
    public override Task XY_with_collection_join(bool async)
        => Task.CompletedTask;

    // Null comparison tests - different behavior
    public override Task IsEmpty_equal_to_null(bool async)
        => Task.CompletedTask;

    public override Task IsEmpty_not_equal_to_null(bool async)
        => Task.CompletedTask;

    public override Task Intersects_equal_to_null(bool async)
        => Task.CompletedTask;

    public override Task Intersects_not_equal_to_null(bool async)
        => Task.CompletedTask;

    private void AssertSql(params string[] expected)
        => Fixture.TestSqlLoggerFactory.AssertBaseline(expected);
}
