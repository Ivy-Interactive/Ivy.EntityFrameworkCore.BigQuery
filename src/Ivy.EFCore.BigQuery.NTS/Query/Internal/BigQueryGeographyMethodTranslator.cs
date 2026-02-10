using System.Reflection;
using Ivy.EntityFrameworkCore.BigQuery.Query.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
using NetTopologySuite.Geometries;

namespace Ivy.EntityFrameworkCore.BigQuery.NetTopologySuite.Query.Internal;

/// <summary>
/// Translates NetTopologySuite Geometry method calls to BigQuery ST_* functions.
/// </summary>
/// <remarks>
/// https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions
/// </remarks>
public class BigQueryGeographyMethodTranslator : IMethodCallTranslator
{
    private readonly ISqlExpressionFactory _sqlExpressionFactory;
    private readonly IRelationalTypeMappingSource _typeMappingSource;

    private static readonly bool[] TrueArrays1 = [true];
    private static readonly bool[] TrueArrays2 = [true, true];
    private static readonly bool[] TrueArrays3 = [true, true, true];

    // Geometry methods
    private static readonly MethodInfo Geometry_AsBinary = typeof(Geometry).GetRuntimeMethod(nameof(Geometry.AsBinary), Type.EmptyTypes)!;
    private static readonly MethodInfo Geometry_AsText = typeof(Geometry).GetRuntimeMethod(nameof(Geometry.AsText), Type.EmptyTypes)!;
    private static readonly MethodInfo Geometry_Buffer = typeof(Geometry).GetRuntimeMethod(nameof(Geometry.Buffer), [typeof(double)])!;
    private static readonly MethodInfo Geometry_Contains = typeof(Geometry).GetRuntimeMethod(nameof(Geometry.Contains), [typeof(Geometry)])!;
    private static readonly MethodInfo Geometry_ConvexHull = typeof(Geometry).GetRuntimeMethod(nameof(Geometry.ConvexHull), Type.EmptyTypes)!;
    private static readonly MethodInfo Geometry_Copy = typeof(Geometry).GetRuntimeMethod(nameof(Geometry.Copy), Type.EmptyTypes)!;
    private static readonly MethodInfo Geometry_CoveredBy = typeof(Geometry).GetRuntimeMethod(nameof(Geometry.CoveredBy), [typeof(Geometry)])!;
    private static readonly MethodInfo Geometry_Covers = typeof(Geometry).GetRuntimeMethod(nameof(Geometry.Covers), [typeof(Geometry)])!;
    private static readonly MethodInfo Geometry_Difference = typeof(Geometry).GetRuntimeMethod(nameof(Geometry.Difference), [typeof(Geometry)])!;
    private static readonly MethodInfo Geometry_Disjoint = typeof(Geometry).GetRuntimeMethod(nameof(Geometry.Disjoint), [typeof(Geometry)])!;
    private static readonly MethodInfo Geometry_Distance = typeof(Geometry).GetRuntimeMethod(nameof(Geometry.Distance), [typeof(Geometry)])!;
    private static readonly MethodInfo Geometry_EqualsTopologically = typeof(Geometry).GetRuntimeMethod(nameof(Geometry.EqualsTopologically), [typeof(Geometry)])!;
    private static readonly MethodInfo Geometry_Intersection = typeof(Geometry).GetRuntimeMethod(nameof(Geometry.Intersection), [typeof(Geometry)])!;
    private static readonly MethodInfo Geometry_Intersects = typeof(Geometry).GetRuntimeMethod(nameof(Geometry.Intersects), [typeof(Geometry)])!;
    private static readonly MethodInfo Geometry_IsWithinDistance = typeof(Geometry).GetRuntimeMethod(nameof(Geometry.IsWithinDistance), [typeof(Geometry), typeof(double)])!;
    private static readonly MethodInfo Geometry_Overlaps = typeof(Geometry).GetRuntimeMethod(nameof(Geometry.Overlaps), [typeof(Geometry)])!;
    private static readonly MethodInfo Geometry_SymmetricDifference = typeof(Geometry).GetRuntimeMethod(nameof(Geometry.SymmetricDifference), [typeof(Geometry)])!;
    private static readonly MethodInfo Geometry_Touches = typeof(Geometry).GetRuntimeMethod(nameof(Geometry.Touches), [typeof(Geometry)])!;
    private static readonly MethodInfo Geometry_Union = typeof(Geometry).GetRuntimeMethod(nameof(Geometry.Union), [typeof(Geometry)])!;
    private static readonly MethodInfo Geometry_Within = typeof(Geometry).GetRuntimeMethod(nameof(Geometry.Within), [typeof(Geometry)])!;

    // LineString methods
    private static readonly MethodInfo LineString_GetPointN = typeof(LineString).GetRuntimeMethod(nameof(LineString.GetPointN), [typeof(int)])!;

    // GeometryCollection methods
    private static readonly MethodInfo GeometryCollection_GetGeometryN = typeof(GeometryCollection).GetRuntimeMethod(nameof(GeometryCollection.GetGeometryN), [typeof(int)])!;

    public BigQueryGeographyMethodTranslator(
        ISqlExpressionFactory sqlExpressionFactory,
        IRelationalTypeMappingSource typeMappingSource)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
        _typeMappingSource = typeMappingSource;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (instance == null)
            return null;

        var declaringType = method.DeclaringType;
        if (declaringType == null || !typeof(Geometry).IsAssignableFrom(declaringType))
            return null;

        var geometryMapping = _typeMappingSource.FindMapping(typeof(Geometry));
        var pointMapping = _typeMappingSource.FindMapping(typeof(Point));

        // Use method.Name matching for robustness (handles derived types like MultiLineString)
        return method.Name switch
        {
            // Methods that return Geometry
            nameof(Geometry.Buffer) => Function("ST_BUFFER", [instance, arguments[0]], typeof(Geometry), geometryMapping),
            nameof(Geometry.ConvexHull) => Function("ST_CONVEXHULL", [instance], typeof(Geometry), geometryMapping),
            nameof(Geometry.Copy) => instance,
            nameof(Geometry.Difference) => Function("ST_DIFFERENCE", [instance, arguments[0]], typeof(Geometry), geometryMapping),
            nameof(Geometry.Intersection) => Function("ST_INTERSECTION", [instance, arguments[0]], typeof(Geometry), geometryMapping),
            // BigQuery doesn't have ST_SYMMETRICDIFFERENCE, use ST_UNION(ST_DIFFERENCE(a, b), ST_DIFFERENCE(b, a))
            nameof(Geometry.SymmetricDifference) => TranslateSymmetricDifference(instance, arguments[0], geometryMapping),
            nameof(Geometry.Union) when arguments.Count == 1 => Function("ST_UNION", [instance, arguments[0]], typeof(Geometry), geometryMapping),

            // Methods that return bool
            nameof(Geometry.Contains) => Function("ST_CONTAINS", [instance, arguments[0]], typeof(bool), null),
            nameof(Geometry.CoveredBy) => Function("ST_COVEREDBY", [instance, arguments[0]], typeof(bool), null),
            nameof(Geometry.Covers) => Function("ST_COVERS", [instance, arguments[0]], typeof(bool), null),
            nameof(Geometry.Disjoint) => Function("ST_DISJOINT", [instance, arguments[0]], typeof(bool), null),
            nameof(Geometry.EqualsTopologically) => Function("ST_EQUALS", [instance, arguments[0]], typeof(bool), null),
            nameof(Geometry.Intersects) => Function("ST_INTERSECTS", [instance, arguments[0]], typeof(bool), null),
            nameof(Geometry.Overlaps) =>
                // BigQuery doesn't have ST_OVERLAPS; use ST_INTERSECTS && !ST_TOUCHES
                _sqlExpressionFactory.AndAlso(
                    Function("ST_INTERSECTS", [instance, arguments[0]], typeof(bool), null),
                    _sqlExpressionFactory.Not(Function("ST_TOUCHES", [instance, arguments[0]], typeof(bool), null))),
            nameof(Geometry.Touches) => Function("ST_TOUCHES", [instance, arguments[0]], typeof(bool), null),
            nameof(Geometry.Within) => Function("ST_WITHIN", [instance, arguments[0]], typeof(bool), null),
            nameof(Geometry.IsWithinDistance) => Function("ST_DWITHIN", [instance, arguments[0], arguments[1]], typeof(bool), null),

            // Methods that return double
            nameof(Geometry.Distance) => Function("ST_DISTANCE", [instance, arguments[0]], typeof(double), null),

            // Methods that return byte[]
            nameof(Geometry.AsBinary) or nameof(Geometry.ToBinary) => Function("ST_ASBINARY", [instance], typeof(byte[]), null),

            // Methods that return string
            nameof(Geometry.AsText) or nameof(Geometry.ToText) => Function("ST_ASTEXT", [instance], typeof(string), null),

            // GetPointN - NTS uses 0-based, BigQuery uses 1-based indexing
            nameof(LineString.GetPointN) => Function("ST_POINTN", [instance, OneBased(arguments[0])], typeof(Point), pointMapping),

            // GetGeometryN - BigQuery doesn't have ST_GEOMETRYN, use ST_DUMP with array access
            // NTS uses 0-based indexing, BigQuery OFFSET is 0-based
            nameof(GeometryCollection.GetGeometryN) => TranslateGetGeometryN(instance, arguments[0], geometryMapping),

            _ => null
        };
    }

    private SqlExpression TranslateSymmetricDifference(SqlExpression a, SqlExpression b, RelationalTypeMapping? geometryMapping)
    {
        // BigQuery doesn't have ST_SYMMETRICDIFFERENCE
        var diffAB = Function("ST_DIFFERENCE", [a, b], typeof(Geometry), geometryMapping);
        var diffBA = Function("ST_DIFFERENCE", [b, a], typeof(Geometry), geometryMapping);
        return Function("ST_UNION", [diffAB, diffBA], typeof(Geometry), geometryMapping);
    }

    private SqlExpression TranslateGetGeometryN(SqlExpression instance, SqlExpression index, RelationalTypeMapping? geometryMapping)
    {
        // BigQuery: ST_DUMP(geog)[OFFSET(index)]
        // ST_DUMP returns an array of geographies, use BigQueryArrayIndexExpression for array access
        var stringMapping = _typeMappingSource.FindMapping(typeof(string));
        var intMapping = _typeMappingSource.FindMapping(typeof(int));
        var dump = _sqlExpressionFactory.Function(
            "ST_DUMP",
            new[] { instance },
            nullable: true,
            argumentsPropagateNullability: TrueArrays1,
            typeof(string),
            stringMapping);
        var typedIndex = _sqlExpressionFactory.ApplyTypeMapping(index, intMapping);
        return new BigQueryArrayIndexExpression(dump, typedIndex, typeof(Geometry), geometryMapping);
    }

    private SqlExpression OneBased(SqlExpression index)
    {
        return _sqlExpressionFactory.Add(index, _sqlExpressionFactory.Constant(1));
    }

    private SqlExpression Function(string name, SqlExpression[] args, Type returnType, RelationalTypeMapping? typeMapping)
    {
        var nullability = args.Length switch
        {
            1 => TrueArrays1,
            2 => TrueArrays2,
            3 => TrueArrays3,
            _ => args.Select(_ => true).ToArray()
        };

        return _sqlExpressionFactory.Function(
            name,
            args,
            nullable: true,
            argumentsPropagateNullability: nullability,
            returnType,
            typeMapping);
    }
}