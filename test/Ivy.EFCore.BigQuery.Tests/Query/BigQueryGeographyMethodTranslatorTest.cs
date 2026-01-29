using System.Linq.Expressions;
using Ivy.EntityFrameworkCore.BigQuery.NetTopologySuite.Query.Internal;
using Ivy.EntityFrameworkCore.BigQuery.NetTopologySuite.Storage.Internal;
using Ivy.EntityFrameworkCore.BigQuery.Query.Internal;
using Ivy.EntityFrameworkCore.BigQuery.Storage.Internal;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.Json;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using NetTopologySuite;
using NetTopologySuite.Geometries;
using Xunit;

#pragma warning disable EF1001 // Internal EF Core API usage.

namespace Ivy.EntityFrameworkCore.BigQuery.Tests.Query;

public class BigQueryGeographyMethodTranslatorTest
{
    private readonly BigQueryGeographyMethodTranslator _translator;
    private readonly BigQuerySqlExpressionFactory _sqlExpressionFactory;
    private readonly IRelationalTypeMappingSource _typeMappingSource;
    private readonly BigQueryNetTopologySuiteTypeMappingSourcePlugin _ntsPlugin;

    public BigQueryGeographyMethodTranslatorTest()
    {
        _typeMappingSource = new BigQueryTypeMappingSource(
            new TypeMappingSourceDependencies(
                new ValueConverterSelector(new ValueConverterSelectorDependencies()),
                new JsonValueReaderWriterSource(new JsonValueReaderWriterSourceDependencies()),
                []),
            new RelationalTypeMappingSourceDependencies([]));

        _ntsPlugin = new BigQueryNetTopologySuiteTypeMappingSourcePlugin(NtsGeometryServices.Instance);

        var model = new Model();
        var dependencies = new SqlExpressionFactoryDependencies(model, _typeMappingSource);
        _sqlExpressionFactory = new BigQuerySqlExpressionFactory(dependencies);

        _translator = new BigQueryGeographyMethodTranslator(_sqlExpressionFactory, _typeMappingSource);
    }

    #region Set Operations - Methods returning Geometry

    [Fact]
    public void Geometry_Buffer_Translates_To_ST_BUFFER()
    {
        var instance = CreateGeometryParameter(typeof(Geometry));
        var distanceArg = CreateDoubleConstant(100.0);
        var method = typeof(Geometry).GetMethod(nameof(Geometry.Buffer), [typeof(double)])!;

        var result = _translator.Translate(instance, method, [distanceArg], null!);

        AssertFunctionTranslation(result, "ST_BUFFER", typeof(Geometry), 2);
    }

    [Fact]
    public void Geometry_ConvexHull_Translates_To_ST_CONVEXHULL()
    {
        var instance = CreateGeometryParameter(typeof(Geometry));
        var method = typeof(Geometry).GetMethod(nameof(Geometry.ConvexHull), Type.EmptyTypes)!;

        var result = _translator.Translate(instance, method, [], null!);

        AssertFunctionTranslation(result, "ST_CONVEXHULL", typeof(Geometry), 1);
    }

    [Fact]
    public void Geometry_Difference_Translates_To_ST_DIFFERENCE()
    {
        var instance = CreateGeometryParameter(typeof(Geometry));
        var otherArg = CreateGeometryParameter(typeof(Geometry));
        var method = typeof(Geometry).GetMethod(nameof(Geometry.Difference), [typeof(Geometry)])!;

        var result = _translator.Translate(instance, method, [otherArg], null!);

        AssertFunctionTranslation(result, "ST_DIFFERENCE", typeof(Geometry), 2);
    }

    [Fact]
    public void Geometry_Intersection_Translates_To_ST_INTERSECTION()
    {
        var instance = CreateGeometryParameter(typeof(Geometry));
        var otherArg = CreateGeometryParameter(typeof(Geometry));
        var method = typeof(Geometry).GetMethod(nameof(Geometry.Intersection), [typeof(Geometry)])!;

        var result = _translator.Translate(instance, method, [otherArg], null!);

        AssertFunctionTranslation(result, "ST_INTERSECTION", typeof(Geometry), 2);
    }

    [Fact]
    public void Geometry_SymmetricDifference_Translates_To_ST_SYMMETRICDIFFERENCE()
    {
        var instance = CreateGeometryParameter(typeof(Geometry));
        var otherArg = CreateGeometryParameter(typeof(Geometry));
        var method = typeof(Geometry).GetMethod(nameof(Geometry.SymmetricDifference), [typeof(Geometry)])!;

        var result = _translator.Translate(instance, method, [otherArg], null!);

        AssertFunctionTranslation(result, "ST_SYMMETRICDIFFERENCE", typeof(Geometry), 2);
    }

    [Fact]
    public void Geometry_Union_Translates_To_ST_UNION()
    {
        var instance = CreateGeometryParameter(typeof(Geometry));
        var otherArg = CreateGeometryParameter(typeof(Geometry));
        var method = typeof(Geometry).GetMethod(nameof(Geometry.Union), [typeof(Geometry)])!;

        var result = _translator.Translate(instance, method, [otherArg], null!);

        AssertFunctionTranslation(result, "ST_UNION", typeof(Geometry), 2);
    }

    [Fact]
    public void Geometry_Copy_Returns_Instance()
    {
        var instance = CreateGeometryParameter(typeof(Geometry));
        var method = typeof(Geometry).GetMethod(nameof(Geometry.Copy), Type.EmptyTypes)!;

        var result = _translator.Translate(instance, method, [], null!);

        Assert.NotNull(result);
        Assert.Same(instance, result);
    }

    #endregion

    #region Predicates - Methods returning bool

    [Fact]
    public void Geometry_Contains_Translates_To_ST_CONTAINS()
    {
        var instance = CreateGeometryParameter(typeof(Geometry));
        var otherArg = CreateGeometryParameter(typeof(Geometry));
        var method = typeof(Geometry).GetMethod(nameof(Geometry.Contains), [typeof(Geometry)])!;

        var result = _translator.Translate(instance, method, [otherArg], null!);

        AssertFunctionTranslation(result, "ST_CONTAINS", typeof(bool), 2);
    }

    [Fact]
    public void Geometry_CoveredBy_Translates_To_ST_COVEREDBY()
    {
        var instance = CreateGeometryParameter(typeof(Geometry));
        var otherArg = CreateGeometryParameter(typeof(Geometry));
        var method = typeof(Geometry).GetMethod(nameof(Geometry.CoveredBy), [typeof(Geometry)])!;

        var result = _translator.Translate(instance, method, [otherArg], null!);

        AssertFunctionTranslation(result, "ST_COVEREDBY", typeof(bool), 2);
    }

    [Fact]
    public void Geometry_Covers_Translates_To_ST_COVERS()
    {
        var instance = CreateGeometryParameter(typeof(Geometry));
        var otherArg = CreateGeometryParameter(typeof(Geometry));
        var method = typeof(Geometry).GetMethod(nameof(Geometry.Covers), [typeof(Geometry)])!;

        var result = _translator.Translate(instance, method, [otherArg], null!);

        AssertFunctionTranslation(result, "ST_COVERS", typeof(bool), 2);
    }

    [Fact]
    public void Geometry_Disjoint_Translates_To_ST_DISJOINT()
    {
        var instance = CreateGeometryParameter(typeof(Geometry));
        var otherArg = CreateGeometryParameter(typeof(Geometry));
        var method = typeof(Geometry).GetMethod(nameof(Geometry.Disjoint), [typeof(Geometry)])!;

        var result = _translator.Translate(instance, method, [otherArg], null!);

        AssertFunctionTranslation(result, "ST_DISJOINT", typeof(bool), 2);
    }

    [Fact]
    public void Geometry_EqualsTopologically_Translates_To_ST_EQUALS()
    {
        var instance = CreateGeometryParameter(typeof(Geometry));
        var otherArg = CreateGeometryParameter(typeof(Geometry));
        var method = typeof(Geometry).GetMethod(nameof(Geometry.EqualsTopologically), [typeof(Geometry)])!;

        var result = _translator.Translate(instance, method, [otherArg], null!);

        AssertFunctionTranslation(result, "ST_EQUALS", typeof(bool), 2);
    }

    [Fact]
    public void Geometry_Intersects_Translates_To_ST_INTERSECTS()
    {
        var instance = CreateGeometryParameter(typeof(Geometry));
        var otherArg = CreateGeometryParameter(typeof(Geometry));
        var method = typeof(Geometry).GetMethod(nameof(Geometry.Intersects), [typeof(Geometry)])!;

        var result = _translator.Translate(instance, method, [otherArg], null!);

        AssertFunctionTranslation(result, "ST_INTERSECTS", typeof(bool), 2);
    }

    [Fact]
    public void Geometry_Overlaps_Translates_To_Combined_Expression()
    {
        // BigQuery doesn't have ST_OVERLAPS, so it's implemented as ST_INTERSECTS && !ST_TOUCHES
        var instance = CreateGeometryParameter(typeof(Geometry));
        var otherArg = CreateGeometryParameter(typeof(Geometry));
        var method = typeof(Geometry).GetMethod(nameof(Geometry.Overlaps), [typeof(Geometry)])!;

        var result = _translator.Translate(instance, method, [otherArg], null!);

        Assert.NotNull(result);
        var andExpression = Assert.IsType<SqlBinaryExpression>(result);
        Assert.Equal(typeof(bool), andExpression.Type);
    }

    [Fact]
    public void Geometry_Touches_Translates_To_ST_TOUCHES()
    {
        var instance = CreateGeometryParameter(typeof(Geometry));
        var otherArg = CreateGeometryParameter(typeof(Geometry));
        var method = typeof(Geometry).GetMethod(nameof(Geometry.Touches), [typeof(Geometry)])!;

        var result = _translator.Translate(instance, method, [otherArg], null!);

        AssertFunctionTranslation(result, "ST_TOUCHES", typeof(bool), 2);
    }

    [Fact]
    public void Geometry_Within_Translates_To_ST_WITHIN()
    {
        var instance = CreateGeometryParameter(typeof(Geometry));
        var otherArg = CreateGeometryParameter(typeof(Geometry));
        var method = typeof(Geometry).GetMethod(nameof(Geometry.Within), [typeof(Geometry)])!;

        var result = _translator.Translate(instance, method, [otherArg], null!);

        AssertFunctionTranslation(result, "ST_WITHIN", typeof(bool), 2);
    }

    [Fact]
    public void Geometry_IsWithinDistance_Translates_To_ST_DWITHIN()
    {
        var instance = CreateGeometryParameter(typeof(Geometry));
        var otherArg = CreateGeometryParameter(typeof(Geometry));
        var distanceArg = CreateDoubleConstant(1000.0);
        var method = typeof(Geometry).GetMethod(nameof(Geometry.IsWithinDistance), [typeof(Geometry), typeof(double)])!;

        var result = _translator.Translate(instance, method, [otherArg, distanceArg], null!);

        AssertFunctionTranslation(result, "ST_DWITHIN", typeof(bool), 3);
    }

    #endregion

    #region Metrics - Methods returning numeric values

    [Fact]
    public void Geometry_Distance_Translates_To_ST_DISTANCE()
    {
        var instance = CreateGeometryParameter(typeof(Geometry));
        var otherArg = CreateGeometryParameter(typeof(Geometry));
        var method = typeof(Geometry).GetMethod(nameof(Geometry.Distance), [typeof(Geometry)])!;

        var result = _translator.Translate(instance, method, [otherArg], null!);

        AssertFunctionTranslation(result, "ST_DISTANCE", typeof(double), 2);
    }

    #endregion

    #region Conversion Methods

    [Fact]
    public void Geometry_AsBinary_Translates_To_ST_ASBINARY()
    {
        var instance = CreateGeometryParameter(typeof(Geometry));
        var method = typeof(Geometry).GetMethod(nameof(Geometry.AsBinary), Type.EmptyTypes)!;

        var result = _translator.Translate(instance, method, [], null!);

        AssertFunctionTranslation(result, "ST_ASBINARY", typeof(byte[]), 1);
    }

    [Fact]
    public void Geometry_AsText_Translates_To_ST_ASTEXT()
    {
        var instance = CreateGeometryParameter(typeof(Geometry));
        var method = typeof(Geometry).GetMethod(nameof(Geometry.AsText), Type.EmptyTypes)!;

        var result = _translator.Translate(instance, method, [], null!);

        AssertFunctionTranslation(result, "ST_ASTEXT", typeof(string), 1);
    }

    #endregion

    #region Indexing Methods

    [Fact]
    public void LineString_GetPointN_Translates_To_ST_POINTN_With_OneBased_Index()
    {
        var instance = CreateGeometryParameter(typeof(LineString));
        var indexArg = CreateIntConstant(0); // NTS uses 0-based
        var method = typeof(LineString).GetMethod(nameof(LineString.GetPointN), [typeof(int)])!;

        var result = _translator.Translate(instance, method, [indexArg], null!);

        Assert.NotNull(result);
        var function = Assert.IsType<SqlFunctionExpression>(result);
        Assert.Equal("ST_POINTN", function.Name);
        Assert.Equal(typeof(Point), function.Type);
        Assert.Equal(2, function.Arguments!.Count);

        // Verify the index is converted to 1-based
        var indexExpression = function.Arguments[1];
        var addExpression = Assert.IsType<SqlBinaryExpression>(indexExpression);
        Assert.Equal(ExpressionType.Add, addExpression.OperatorType);
    }

    [Fact]
    public void GeometryCollection_GetGeometryN_Translates_To_ST_GEOMETRYN_With_OneBased_Index()
    {
        var instance = CreateGeometryParameter(typeof(GeometryCollection));
        var indexArg = CreateIntConstant(0); // NTS uses 0-based
        var method = typeof(GeometryCollection).GetMethod(nameof(GeometryCollection.GetGeometryN), [typeof(int)])!;

        var result = _translator.Translate(instance, method, [indexArg], null!);

        Assert.NotNull(result);
        var function = Assert.IsType<SqlFunctionExpression>(result);
        Assert.Equal("ST_GEOMETRYN", function.Name);
        Assert.Equal(typeof(Geometry), function.Type);
        Assert.Equal(2, function.Arguments!.Count);

        // Verify the index is converted to 1-based
        var indexExpression = function.Arguments[1];
        var addExpression = Assert.IsType<SqlBinaryExpression>(indexExpression);
        Assert.Equal(ExpressionType.Add, addExpression.OperatorType);
    }

    #endregion

    #region Null and Invalid Input Tests

    [Fact]
    public void Translate_NullInstance_ReturnsNull()
    {
        var method = typeof(Geometry).GetMethod(nameof(Geometry.Buffer), [typeof(double)])!;
        var distanceArg = CreateDoubleConstant(100.0);

        var result = _translator.Translate(null, method, [distanceArg], null!);

        Assert.Null(result);
    }

    [Fact]
    public void Translate_NonGeometryType_ReturnsNull()
    {
        var stringTypeMapping = _typeMappingSource.FindMapping(typeof(string));
        var instance = new SqlParameterExpression("p0", typeof(string), stringTypeMapping);
        var method = typeof(string).GetMethod(nameof(string.Contains), [typeof(string)])!;
        var arg = new SqlParameterExpression("p1", typeof(string), stringTypeMapping);

        var result = _translator.Translate(instance, method, [arg], null!);

        Assert.Null(result);
    }

    #endregion

    #region Helper Methods

    private SqlParameterExpression CreateGeometryParameter(Type geometryType)
    {
        var typeMapping = _ntsPlugin.FindMapping(new RelationalTypeMappingInfo(geometryType));
        return new SqlParameterExpression("p0", geometryType, typeMapping);
    }

    private SqlConstantExpression CreateDoubleConstant(double value)
    {
        var typeMapping = _typeMappingSource.FindMapping(typeof(double));
        return new SqlConstantExpression(value, typeMapping);
    }

    private SqlConstantExpression CreateIntConstant(int value)
    {
        var typeMapping = _typeMappingSource.FindMapping(typeof(int));
        return new SqlConstantExpression(value, typeMapping);
    }

    private static void AssertFunctionTranslation(SqlExpression? result, string expectedFunctionName, Type expectedReturnType, int expectedArgumentCount)
    {
        Assert.NotNull(result);
        var function = Assert.IsType<SqlFunctionExpression>(result);
        Assert.Equal(expectedFunctionName, function.Name);
        Assert.Equal(expectedArgumentCount, function.Arguments!.Count);
        Assert.Equal(expectedReturnType, function.Type);
    }

    #endregion
}
