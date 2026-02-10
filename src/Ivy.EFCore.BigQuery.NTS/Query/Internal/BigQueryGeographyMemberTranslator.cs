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
    private static readonly bool[] TrueArrays3 = [true, true, true];

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

        // The declaring type is the interface, not a Geometry type, so handle before the type check
        if (member.Name == "Count")
        {
            if (IsGeometryCollectionInstance(instance))
                return Function("ST_NUMGEOMETRIES", instance, typeof(int), null);
            if (IsLineStringInstance(instance))
                return Function("ST_NUMPOINTS", instance, typeof(int), null);
        }

        if (declaringType == null || !typeof(Geometry).IsAssignableFrom(declaringType))
            return null;

        var geometryMapping = _typeMappingSource.FindMapping(typeof(Geometry));
        var pointMapping = _typeMappingSource.FindMapping(typeof(Point));
        var lineStringMapping = _typeMappingSource.FindMapping(typeof(LineString));
        
        return member.Name switch
        {
            // Geometry properties
            nameof(Geometry.Area) => Function("ST_AREA", instance, typeof(double), null),
            nameof(Geometry.Centroid) => Function("ST_CENTROID", instance, typeof(Point), pointMapping),
            nameof(Geometry.Dimension) => Function("ST_DIMENSION", instance, typeof(Dimension), null),
            nameof(Geometry.GeometryType) => TranslateGeometryType(instance),
            nameof(Geometry.IsEmpty) => Function("ST_ISEMPTY", instance, typeof(bool), null),
            nameof(Geometry.Length) => Function("ST_LENGTH", instance, typeof(double), null),
            nameof(Geometry.NumGeometries) => Function("ST_NUMGEOMETRIES", instance, typeof(int), null),
            nameof(Geometry.NumPoints) => Function("ST_NUMPOINTS", instance, typeof(int), null),
          
            nameof(Point.X) when typeof(Point).IsAssignableFrom(declaringType)
                => Function("ST_X", instance, typeof(double), null),
            nameof(Point.Y) when typeof(Point).IsAssignableFrom(declaringType)
                => Function("ST_Y", instance, typeof(double), null),            

            nameof(LineString.StartPoint) when typeof(LineString).IsAssignableFrom(declaringType)
                => Function("ST_STARTPOINT", instance, typeof(Point), pointMapping),
            nameof(LineString.EndPoint) when typeof(LineString).IsAssignableFrom(declaringType)
                => Function("ST_ENDPOINT", instance, typeof(Point), pointMapping),
            nameof(LineString.IsClosed) when typeof(LineString).IsAssignableFrom(declaringType)
                => Function("ST_ISCLOSED", instance, typeof(bool), null),
            nameof(LineString.IsRing) when typeof(LineString).IsAssignableFrom(declaringType)
                => Function("ST_ISRING", instance, typeof(bool), null),

            nameof(Polygon.ExteriorRing) when typeof(Polygon).IsAssignableFrom(declaringType)
                => Function("ST_EXTERIORRING", instance, typeof(LineString), lineStringMapping),
            nameof(Polygon.NumInteriorRings) when typeof(Polygon).IsAssignableFrom(declaringType)
                => TranslateNumInteriorRings(instance),

            _ => null
        };
    }

    private SqlExpression TranslateNumInteriorRings(SqlExpression instance)
    {
        // BigQuery doesn't have ST_NUMINTERIORRING, use ARRAY_LENGTH(ST_INTERIORRINGS(geog))
        // ST_INTERIORRINGS returns ARRAY<GEOGRAPHY>, use string type mapping as placeholder
        var stringMapping = _typeMappingSource.FindMapping(typeof(string));
        var interiorRings = _sqlExpressionFactory.Function(
            "ST_INTERIORRINGS",
            new[] { instance },
            nullable: true,
            argumentsPropagateNullability: TrueArrays1,
            typeof(string),
            stringMapping);
        return _sqlExpressionFactory.Function(
            "ARRAY_LENGTH",
            new SqlExpression[] { interiorRings },
            nullable: true,
            argumentsPropagateNullability: TrueArrays1,
            typeof(int));
    }

    private SqlExpression TranslateGeometryType(SqlExpression instance)
    {
        // BigQuery returns "ST_Point", "ST_LineString", etc. but NTS expects "Point", "LineString"
        // Use REPLACE to strip the "ST_" prefix
        var stGeometryType = Function("ST_GEOMETRYTYPE", instance, typeof(string), null);
        return _sqlExpressionFactory.Function(
            "REPLACE",
            new SqlExpression[] { stGeometryType, _sqlExpressionFactory.Constant("ST_"), _sqlExpressionFactory.Constant("") },
            nullable: true,
            argumentsPropagateNullability: TrueArrays3,
            typeof(string));
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

    private static bool IsGeometryCollectionInstance(SqlExpression instance)
    {
        var clrType = instance.TypeMapping?.ClrType;
        return clrType != null && typeof(GeometryCollection).IsAssignableFrom(clrType);
    }

    private static bool IsLineStringInstance(SqlExpression instance)
    {
        var clrType = instance.TypeMapping?.ClrType;
        return clrType != null && typeof(LineString).IsAssignableFrom(clrType);
    }
}