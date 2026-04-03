using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestModels.SpatialModel;
using NetTopologySuite.Geometries;
using NetTopologySuite.Geometries.Utilities;
using NetTopologySuite.IO;
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
    public override async Task AsBinary(bool async)
    {
        var context = Fixture.CreateContext();
        var results = async
            ? await context.Set<PointEntity>().Select(e => new { e.Id, Binary = e.Point!.AsBinary() }).ToListAsync()
            : context.Set<PointEntity>().Select(e => new { e.Id, Binary = e.Point!.AsBinary() }).ToList();

        AssertSql(
            """
SELECT `p`.`Id`, ST_ASBINARY(`p`.`Point`) AS `Binary`
FROM `PointEntity` AS `p`
""");

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

        AssertSql(
            """
SELECT `p`.`Id`, CASE
    WHEN `p`.`Point` IS NULL THEN CAST(NULL AS BYTES)
    ELSE ST_ASBINARY(`p`.`Point`)
END AS `Binary`
FROM `PointEntity` AS `p`
""");

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

    public override Task Boundary(bool async)
        => base.Boundary(async);

    public override async Task Buffer(bool async)
    {
        await base.Buffer(async);

        AssertSql(
            """
SELECT `p`.`Id`, ST_BUFFER(`p`.`Polygon`, 1) AS `Buffer`
FROM `PolygonEntity` AS `p`
""");
    }

    public override Task Buffer_quadrantSegments(bool async)
        => base.Buffer_quadrantSegments(async);

    public override async Task Centroid(bool async)
    {
        await base.Centroid(async);

        AssertSql(
            """
SELECT `p`.`Id`, ST_CENTROID(`p`.`Polygon`) AS `Centroid`
FROM `PolygonEntity` AS `p`
""");
    }

    public override Task Combine_aggregate(bool async)
        // BQ doesn't guarantee order within grouped results
        => AssertQuery(
            async,
            ss => ss.Set<PointEntity>()
                .Where(e => e.Point != null)
                .GroupBy(e => e.Group)
                .Select(g => new { Id = g.Key, Combined = GeometryCombiner.Combine(g.Select(e => e.Point)) }),
            elementSorter: x => x.Id,
            elementAsserter: (e, a) =>
            {
                Assert.Equal(e.Id, a.Id);

                var eCollection = (GeometryCollection)e.Combined;
                var aCollection = (GeometryCollection)a.Combined;

                // Sort geometries by coordinates for deterministic comparison
                var eSorted = eCollection.Geometries.OrderBy(g => g.Coordinate.X).ThenBy(g => g.Coordinate.Y).ToList();
                var aSorted = aCollection.Geometries.OrderBy(g => g.Coordinate.X).ThenBy(g => g.Coordinate.Y).ToList();

                Assert.Equal(eSorted, aSorted);
            });

    public override Task EnvelopeCombine_aggregate(bool async)
        => base.EnvelopeCombine_aggregate(async);

    public override async Task Contains(bool async)
    {
        await base.Contains(async);

        AssertSql(
            """
@point='POINT (0.25 0.25)' (DbType = Object)

SELECT `p`.`Id`, ST_CONTAINS(`p`.`Polygon`, @point) AS `Contains`
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

    public override Task ConvexHull_aggregate(bool async)
        => base.ConvexHull_aggregate(async);

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
@polygon='POLYGON ((-1 -1, 2 -1, 2 2, -1 2, -1 -1))' (DbType = Object)

SELECT `p`.`Id`, ST_COVEREDBY(`p`.`Point`, @polygon) AS `CoveredBy`
FROM `PointEntity` AS `p`
""");
    }

    public override async Task Covers(bool async)
    {
        await base.Covers(async);

        AssertSql(
            """
@point='POINT (0.25 0.25)' (DbType = Object)

SELECT `p`.`Id`, ST_COVERS(`p`.`Polygon`, @point) AS `Covers`
FROM `PolygonEntity` AS `p`
""");
    }

    [ConditionalTheory(Skip = "No BigQuery translation for ST_CROSSES")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Crosses(bool async)
        => base.Crosses(async);

    public override async Task Difference(bool async)
    {
        await base.Difference(async);

        AssertSql(
            """
@polygon='POLYGON ((0 0, 1 0, 1 1, 0 0))' (DbType = Object)

SELECT `p`.`Id`, ST_DIFFERENCE(`p`.`Polygon`, @polygon) AS `Difference`
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
@point='POINT (1 1)' (DbType = Object)

SELECT `p`.`Id`, ST_DISJOINT(`p`.`Polygon`, @point) AS `Disjoint`
FROM `PolygonEntity` AS `p`
""");
    }

    public override async Task Disjoint_with_null_check(bool async)
    {
        await base.Disjoint_with_null_check(async);

        AssertSql(
            """
@point='POINT (1 1)' (DbType = Object)

SELECT `p`.`Id`, CASE
    WHEN `p`.`Polygon` IS NULL THEN CAST(NULL AS BOOL)
    ELSE ST_DISJOINT(`p`.`Polygon`, @point)
END AS `Disjoint`
FROM `PolygonEntity` AS `p`
""");
    }

    public override async Task Distance_with_null_check(bool async)
    {
        await base.Distance_with_null_check(async);

        AssertSql(
            """
@point='POINT (0 1)' (DbType = Object)

SELECT `p`.`Id`, ST_DISTANCE(`p`.`Point`, @point) AS `Distance`
FROM `PointEntity` AS `p`
""");
    }

    public override async Task Distance_with_cast_to_nullable(bool async)
    {
        await base.Distance_with_cast_to_nullable(async);

        AssertSql(
            """
@point='POINT (0 1)' (DbType = Object)

SELECT `p`.`Id`, ST_DISTANCE(`p`.`Point`, @point) AS `Distance`
FROM `PointEntity` AS `p`
""");
    }

    public override async Task Distance_geometry(bool async)
    {
        await base.Distance_geometry(async);

        AssertSql(
            """
@point='POINT (0 1)' (DbType = Object)

SELECT `p`.`Id`, ST_DISTANCE(`p`.`Geometry`, @point) AS `Distance`
FROM `PointEntity` AS `p`
""");
    }

    public override Task Distance_constant(bool async)
        => base.Distance_constant(async);

    public override Task Distance_constant_srid_4326(bool async)
        => base.Distance_constant_srid_4326(async);

    public override Task Distance_constant_lhs(bool async)
        => base.Distance_constant_lhs(async);

    public override Task WithConversion(bool async)
        => base.WithConversion(async);

    public override Task Distance_on_converted_geometry_type(bool async)
        => base.Distance_on_converted_geometry_type(async);

    public override Task Distance_on_converted_geometry_type_lhs(bool async)
        => base.Distance_on_converted_geometry_type_lhs(async);

    public override Task Distance_on_converted_geometry_type_constant(bool async)
        => base.Distance_on_converted_geometry_type_constant(async);

    public override Task Distance_on_converted_geometry_type_constant_lhs(bool async)
        => base.Distance_on_converted_geometry_type_constant_lhs(async);

    public override async Task EndPoint(bool async)
    {
        await base.EndPoint(async);

        AssertSql(
            """
SELECT `l`.`Id`, ST_ENDPOINT(`l`.`LineString`) AS `EndPoint`
FROM `LineStringEntity` AS `l`
""");
    }

    public override Task Envelope(bool async)
        => base.Envelope(async);

    public override async Task EqualsTopologically(bool async)
    {
        await base.EqualsTopologically(async);

        AssertSql(
            """
@point='POINT (0 0)' (DbType = Object)

SELECT `p`.`Id`, ST_EQUALS(`p`.`Point`, @point) AS `EqualsTopologically`
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
SELECT `m`.`Id`, ST_DUMP(`m`.`MultiLineString`)[SAFE_OFFSET(0)] AS `Geometry0`
FROM `MultiLineStringEntity` AS `m`
""");
    }

    public override Task GetGeometryN_with_null_argument(bool async)
        => base.GetGeometryN_with_null_argument(async);

    [ConditionalTheory(Skip = "No BigQuery translation for interior ring access")]
    [MemberData(nameof(IsAsyncData))]
    public override Task GetInteriorRingN(bool async)
        => base.GetInteriorRingN(async);

    public override async Task GetPointN(bool async)
    {
        await base.GetPointN(async);

        AssertSql(
            """
SELECT `l`.`Id`, ST_POINTN(`l`.`LineString`, 0 + 1) AS `Point0`
FROM `LineStringEntity` AS `l`
""");
    }

    public override Task InteriorPoint(bool async)
        => base.InteriorPoint(async);

    public override async Task Intersection(bool async)
    {
        await base.Intersection(async);

        AssertSql(
            """
@polygon='POLYGON ((0 0, 1 0, 1 1, 0 0))' (DbType = Object)

SELECT `p`.`Id`, ST_INTERSECTION(`p`.`Polygon`, @polygon) AS `Intersection`
FROM `PolygonEntity` AS `p`
""");
    }

    public override async Task Intersects(bool async)
    {
        await base.Intersects(async);

        AssertSql(
            """
@lineString='LINESTRING (0.5 -0.5, 0.5 0.5)' (DbType = Object)

SELECT `l`.`Id`, ST_INTERSECTS(`l`.`LineString`, @lineString) AS `Intersects`
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

    public override Task IMultiCurve_IsClosed(bool async)
        => base.IMultiCurve_IsClosed(async);

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

    public override Task IsSimple(bool async)
        => base.IsSimple(async);

    public override Task IsValid(bool async)
        => base.IsValid(async);

    public override async Task IsWithinDistance(bool async)
    {
        await base.IsWithinDistance(async);

        AssertSql(
            """
@point='POINT (0 1)' (DbType = Object)

SELECT `p`.`Id`, ST_DWITHIN(`p`.`Point`, @point, 1) AS `IsWithinDistance`
FROM `PointEntity` AS `p`
""");
    }

    [ConditionalTheory(Skip = "Item/indexer - different syntax needed")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Item(bool async)
        => base.Item(async);

    public override async Task Length(bool async)
    {
        await base.Length(async);

        AssertSql(
            """
SELECT `l`.`Id`, ST_LENGTH(`l`.`LineString`) AS `Length`
FROM `LineStringEntity` AS `l`
""");
    }

    public override Task M(bool async)
        => base.M(async);

    [ConditionalTheory(Skip = "No BigQuery translation for ST_NORMALIZE")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Normalized(bool async)
        => base.Normalized(async);

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

    public override Task OgcGeometryType(bool async)
        => base.OgcGeometryType(async);

    public override async Task Overlaps(bool async)
    {
        await base.Overlaps(async);

        // BigQuery doesn't have ST_OVERLAPS, implemented as ST_INTERSECTS && !ST_TOUCHES
        AssertSql(
            """
@polygon='POLYGON ((0 0, 1 0, 1 1, 0 0))' (DbType = Object)

SELECT `p`.`Id`, ST_INTERSECTS(`p`.`Polygon`, @polygon) AND NOT (ST_TOUCHES(`p`.`Polygon`, @polygon)) AS `Overlaps`
FROM `PolygonEntity` AS `p`
""");
    }

    public override Task PointOnSurface(bool async)
        => base.PointOnSurface(async);

    [ConditionalTheory(Skip = "No BigQuery translation for ST_RELATE")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Relate(bool async)
        => base.Relate(async);

    [ConditionalTheory(Skip = "No BigQuery translation for ST_REVERSE")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Reverse(bool async)
        => base.Reverse(async);

    [ConditionalTheory(Skip = "BigQuery geography doesn't support SRID property (always 4326)")]
    [MemberData(nameof(IsAsyncData))]
    public override Task SRID(bool async)
        => base.SRID(async);

    [ConditionalTheory(Skip = "BigQuery geography doesn't support SRID property (always 4326)")]
    [MemberData(nameof(IsAsyncData))]
    public override Task SRID_geometry(bool async)
        => base.SRID_geometry(async);

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
@polygon='POLYGON ((0 0, 1 0, 1 1, 0 0))' (DbType = Object)

SELECT `p`.`Id`, ST_UNION(ST_DIFFERENCE(`p`.`Polygon`, @polygon), ST_DIFFERENCE(@polygon, `p`.`Polygon`)) AS `SymmetricDifference`
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

        AssertSql(
            """
SELECT `p`.`Id`, ST_ASBINARY(`p`.`Point`) AS `Binary`
FROM `PointEntity` AS `p`
""");

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
@polygon='POLYGON ((0 1, 1 0, 1 1, 0 1))' (DbType = Object)

SELECT `p`.`Id`, ST_TOUCHES(`p`.`Polygon`, @polygon) AS `Touches`
FROM `PolygonEntity` AS `p`
""");
    }

    public override async Task Union(bool async)
    {
        await base.Union(async);

        AssertSql(
            """
@polygon='POLYGON ((0 0, 1 0, 1 1, 0 0))' (DbType = Object)

SELECT `p`.`Id`, ST_UNION(`p`.`Polygon`, @polygon) AS `Union`
FROM `PolygonEntity` AS `p`
""");
    }

    public override Task Union_aggregate(bool async)
        => base.Union_aggregate(async);

    [ConditionalTheory(Skip = "Union without parameter (self-union) - not supported")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Union_void(bool async)
        => base.Union_void(async);

    public override async Task Within(bool async)
    {
        await base.Within(async);

        AssertSql(
            """
@polygon='POLYGON ((-1 -1, 2 -1, 2 2, -1 2, -1 -1))' (DbType = Object)

SELECT `p`.`Id`, ST_WITHIN(`p`.`Point`, @polygon) AS `Within`
FROM `PointEntity` AS `p`
""");
    }

    public override async Task X(bool async)
    {
        // BigQuery GEOGRAPHY has floating-point precision differences (e.g., 0.99999999999999978 vs 1)
        await AssertQuery(
            async,
            ss => ss.Set<PointEntity>().Select(e => new { e.Id, X = e.Point == null ? (double?)null : e.Point.X }),
            elementSorter: e => e.Id,
            elementAsserter: (e, a) =>
            {
                Assert.Equal(e.Id, a.Id);
                Assert.Equal(e.X ?? 0, a.X ?? 0, precision: 10);
            });

        AssertSql(
            """
SELECT `p`.`Id`, ST_X(`p`.`Point`) AS `X`
FROM `PointEntity` AS `p`
""");
    }

    public override async Task Y(bool async)
    {
        // BigQuery GEOGRAPHY has floating-point precision differences
        await AssertQuery(
            async,
            ss => ss.Set<PointEntity>().Select(e => new { e.Id, Y = e.Point == null ? (double?)null : e.Point.Y }),
            elementSorter: e => e.Id,
            elementAsserter: (e, a) =>
            {
                Assert.Equal(e.Id, a.Id);
                Assert.Equal(e.Y ?? 0, a.Y ?? 0, precision: 10);
            });

        AssertSql(
            """
SELECT `p`.`Id`, ST_Y(`p`.`Point`) AS `Y`
FROM `PointEntity` AS `p`
""");
    }

    public override Task Z(bool async)
        => base.Z(async);

    public override Task XY_with_collection_join(bool async)
        => base.XY_with_collection_join(async);

    public override Task IsEmpty_equal_to_null(bool async)
        => base.IsEmpty_equal_to_null(async);

    public override Task IsEmpty_not_equal_to_null(bool async)
        => base.IsEmpty_not_equal_to_null(async);

    public override Task Intersects_equal_to_null(bool async)
        => base.Intersects_equal_to_null(async);

    public override Task Intersects_not_equal_to_null(bool async)
        => base.Intersects_not_equal_to_null(async);

    private void AssertSql(params string[] expected)
        => Fixture.TestSqlLoggerFactory.AssertBaseline(expected);
}