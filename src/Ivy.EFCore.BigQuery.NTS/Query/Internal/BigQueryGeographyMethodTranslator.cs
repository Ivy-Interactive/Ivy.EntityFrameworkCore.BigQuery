using System.Reflection;
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

        // Methods that return Geometry
        if (method == Geometry_Buffer)
            return Function("ST_BUFFER", [instance, arguments[0]], typeof(Geometry), geometryMapping);

        if (method == Geometry_ConvexHull)
            return Function("ST_CONVEXHULL", [instance], typeof(Geometry), geometryMapping);

        if (method == Geometry_Copy)
            return instance;

        if (method == Geometry_Difference)
            return Function("ST_DIFFERENCE", [instance, arguments[0]], typeof(Geometry), geometryMapping);

        if (method == Geometry_Intersection)
            return Function("ST_INTERSECTION", [instance, arguments[0]], typeof(Geometry), geometryMapping);

        if (method == Geometry_SymmetricDifference)
            return Function("ST_SYMMETRICDIFFERENCE", [instance, arguments[0]], typeof(Geometry), geometryMapping);

        if (method == Geometry_Union)
            return Function("ST_UNION", [instance, arguments[0]], typeof(Geometry), geometryMapping);

        // Methods that return bool
        if (method == Geometry_Contains)
            return Function("ST_CONTAINS", [instance, arguments[0]], typeof(bool), null);

        if (method == Geometry_CoveredBy)
            return Function("ST_COVEREDBY", [instance, arguments[0]], typeof(bool), null);

        if (method == Geometry_Covers)
            return Function("ST_COVERS", [instance, arguments[0]], typeof(bool), null);

        if (method == Geometry_Disjoint)
            return Function("ST_DISJOINT", [instance, arguments[0]], typeof(bool), null);

        if (method == Geometry_EqualsTopologically)
            return Function("ST_EQUALS", [instance, arguments[0]], typeof(bool), null);

        if (method == Geometry_Intersects)
            return Function("ST_INTERSECTS", [instance, arguments[0]], typeof(bool), null);

        if (method == Geometry_Overlaps)
            // BQdoesn't have ST_OVERLAPS; use ST_INTERSECTS && !ST_TOUCHES
            return _sqlExpressionFactory.AndAlso(
                Function("ST_INTERSECTS", [instance, arguments[0]], typeof(bool), null),
                _sqlExpressionFactory.Not(Function("ST_TOUCHES", [instance, arguments[0]], typeof(bool), null)));

        if (method == Geometry_Touches)
            return Function("ST_TOUCHES", [instance, arguments[0]], typeof(bool), null);

        if (method == Geometry_Within)
            return Function("ST_WITHIN", [instance, arguments[0]], typeof(bool), null);

        if (method == Geometry_IsWithinDistance)
            return Function("ST_DWITHIN", [instance, arguments[0], arguments[1]], typeof(bool), null);

        // Methods that return double
        if (method == Geometry_Distance)
            return Function("ST_DISTANCE", [instance, arguments[0]], typeof(double), null);

        // Methods that return byte[]
        if (method == Geometry_AsBinary)
            return Function("ST_ASBINARY", [instance], typeof(byte[]), null);

        // Methods that return string
        if (method == Geometry_AsText)
            return Function("ST_ASTEXT", [instance], typeof(string), null);

        // LineString.GetPointN - note: NTS uses 0-based, BigQuery uses 1-based indexing
        if (method == LineString_GetPointN)
        {
            var pointMapping = _typeMappingSource.FindMapping(typeof(Point));
            var oneBasedIndex = _sqlExpressionFactory.Add(arguments[0], _sqlExpressionFactory.Constant(1));
            return Function("ST_POINTN", [instance, oneBasedIndex], typeof(Point), pointMapping);
        }

        // GeometryCollection.GetGeometryN. NTS uses 0-based, BigQuery uses 1-based indexing
        if (method == GeometryCollection_GetGeometryN)
        {
            var oneBasedIndex = _sqlExpressionFactory.Add(arguments[0], _sqlExpressionFactory.Constant(1));
            return Function("ST_GEOMETRYN", [instance, oneBasedIndex], typeof(Geometry), geometryMapping);
        }

        return null;
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