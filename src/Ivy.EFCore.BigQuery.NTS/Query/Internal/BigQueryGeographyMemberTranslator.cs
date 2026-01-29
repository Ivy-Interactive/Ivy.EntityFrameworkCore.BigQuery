using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
using NetTopologySuite.Geometries;

namespace Ivy.EntityFrameworkCore.BigQuery.NetTopologySuite.Query.Internal;

/// <summary>
/// Translates NetTopologySuite Geometry property access to BigQuery ST_* functions.
/// </summary>
/// <remarks>
/// https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions
/// </remarks>
public class BigQueryGeographyMemberTranslator : IMemberTranslator
{
    private readonly ISqlExpressionFactory _sqlExpressionFactory;
    private readonly IRelationalTypeMappingSource _typeMappingSource;

    private static readonly bool[] TrueArrays1 = [true];

    // Geometry properties
    private static readonly MemberInfo Geometry_Area = typeof(Geometry).GetProperty(nameof(Geometry.Area))!;
    private static readonly MemberInfo Geometry_Centroid = typeof(Geometry).GetProperty(nameof(Geometry.Centroid))!;
    private static readonly MemberInfo Geometry_Dimension = typeof(Geometry).GetProperty(nameof(Geometry.Dimension))!;
    private static readonly MemberInfo Geometry_GeometryType = typeof(Geometry).GetProperty(nameof(Geometry.GeometryType))!;
    private static readonly MemberInfo Geometry_IsEmpty = typeof(Geometry).GetProperty(nameof(Geometry.IsEmpty))!;
    private static readonly MemberInfo Geometry_Length = typeof(Geometry).GetProperty(nameof(Geometry.Length))!;
    private static readonly MemberInfo Geometry_NumGeometries = typeof(Geometry).GetProperty(nameof(Geometry.NumGeometries))!;
    private static readonly MemberInfo Geometry_NumPoints = typeof(Geometry).GetProperty(nameof(Geometry.NumPoints))!;

    // Point properties
    private static readonly MemberInfo Point_X = typeof(Point).GetProperty(nameof(Point.X))!;
    private static readonly MemberInfo Point_Y = typeof(Point).GetProperty(nameof(Point.Y))!;
    private static readonly MemberInfo Point_Z = typeof(Point).GetProperty(nameof(Point.Z))!;
    private static readonly MemberInfo Point_M = typeof(Point).GetProperty(nameof(Point.M))!;

    // LineString properties
    private static readonly MemberInfo LineString_StartPoint = typeof(LineString).GetProperty(nameof(LineString.StartPoint))!;
    private static readonly MemberInfo LineString_EndPoint = typeof(LineString).GetProperty(nameof(LineString.EndPoint))!;
    private static readonly MemberInfo LineString_IsClosed = typeof(LineString).GetProperty(nameof(LineString.IsClosed))!;
    private static readonly MemberInfo LineString_IsRing = typeof(LineString).GetProperty(nameof(LineString.IsRing))!;
    private static readonly MemberInfo LineString_NumPoints = typeof(LineString).GetProperty(nameof(LineString.NumPoints))!;

    // Polygon properties
    private static readonly MemberInfo Polygon_ExteriorRing = typeof(Polygon).GetProperty(nameof(Polygon.ExteriorRing))!;
    private static readonly MemberInfo Polygon_NumInteriorRings = typeof(Polygon).GetProperty(nameof(Polygon.NumInteriorRings))!;

    public BigQueryGeographyMemberTranslator(
        ISqlExpressionFactory sqlExpressionFactory,
        IRelationalTypeMappingSource typeMappingSource)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
        _typeMappingSource = typeMappingSource;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MemberInfo member,
        Type returnType,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (instance == null)
            return null;

        var declaringType = member.DeclaringType;
        if (declaringType == null || !typeof(Geometry).IsAssignableFrom(declaringType))
            return null;

        var geometryMapping = _typeMappingSource.FindMapping(typeof(Geometry));
        var pointMapping = _typeMappingSource.FindMapping(typeof(Point));
        var lineStringMapping = _typeMappingSource.FindMapping(typeof(LineString));

        // Geometry properties
        if (Equals(member, Geometry_Area))
            return Function("ST_AREA", instance, typeof(double), null);

        if (Equals(member, Geometry_Centroid))
            return Function("ST_CENTROID", instance, typeof(Point), pointMapping);

        if (Equals(member, Geometry_Dimension))
            return Function("ST_DIMENSION", instance, typeof(int), null);

        if (Equals(member, Geometry_GeometryType))
            return Function("ST_GEOMETRYTYPE", instance, typeof(string), null);

        if (Equals(member, Geometry_IsEmpty))
            return Function("ST_ISEMPTY", instance, typeof(bool), null);

        if (Equals(member, Geometry_Length))
            return Function("ST_LENGTH", instance, typeof(double), null);

        if (Equals(member, Geometry_NumGeometries))
            return Function("ST_NUMGEOMETRIES", instance, typeof(int), null);

        if (Equals(member, Geometry_NumPoints))
            return Function("ST_NUMPOINTS", instance, typeof(int), null);

        // Point properties
        if (Equals(member, Point_X))
            return Function("ST_X", instance, typeof(double), null);

        if (Equals(member, Point_Y))
            return Function("ST_Y", instance, typeof(double), null);

        // BigQuery doesn't support Z and M coordinates in geography mode
        // Return null for these to let EF Core handle them client-side
        if (Equals(member, Point_Z) || Equals(member, Point_M))
            return null;

        // LineString properties
        if (Equals(member, LineString_StartPoint))
            return Function("ST_STARTPOINT", instance, typeof(Point), pointMapping);

        if (Equals(member, LineString_EndPoint))
            return Function("ST_ENDPOINT", instance, typeof(Point), pointMapping);

        if (Equals(member, LineString_IsClosed))
            return Function("ST_ISCLOSED", instance, typeof(bool), null);

        if (Equals(member, LineString_IsRing))
            return Function("ST_ISRING", instance, typeof(bool), null);

        if (Equals(member, LineString_NumPoints))
            return Function("ST_NUMPOINTS", instance, typeof(int), null);

        // Polygon properties
        if (Equals(member, Polygon_ExteriorRing))
            return Function("ST_EXTERIORRING", instance, typeof(LineString), lineStringMapping);

        if (Equals(member, Polygon_NumInteriorRings))
            return Function("ST_NUMINTERIORRING", instance, typeof(int), null);

        return null;
    }

    private SqlExpression Function(string name, SqlExpression instance, Type returnType, RelationalTypeMapping? typeMapping)
    {
        return _sqlExpressionFactory.Function(
            name,
            [instance],
            nullable: true,
            argumentsPropagateNullability: TrueArrays1,
            returnType,
            typeMapping);
    }
}