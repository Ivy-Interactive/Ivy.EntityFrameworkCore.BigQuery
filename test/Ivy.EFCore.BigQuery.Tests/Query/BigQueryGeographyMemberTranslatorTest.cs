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

public class BigQueryGeographyMemberTranslatorTest
{
    private readonly BigQueryGeographyMemberTranslator _translator;
    private readonly BigQuerySqlExpressionFactory _sqlExpressionFactory;
    private readonly IRelationalTypeMappingSource _typeMappingSource;
    private readonly BigQueryNetTopologySuiteTypeMappingSourcePlugin _ntsPlugin;

    public BigQueryGeographyMemberTranslatorTest()
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

        _translator = new BigQueryGeographyMemberTranslator(_sqlExpressionFactory, _typeMappingSource);
    }

    #region Geometry Properties

    [Fact]
    public void Geometry_Area_Translates_To_ST_AREA()
    {
        var instance = CreateGeometryParameter(typeof(Geometry));
        var member = typeof(Geometry).GetProperty(nameof(Geometry.Area))!;

        var result = _translator.Translate(instance, member, typeof(double), null!);

        AssertFunctionTranslation(result, "ST_AREA", typeof(double));
    }

    [Fact]
    public void Geometry_Centroid_Translates_To_ST_CENTROID()
    {
        var instance = CreateGeometryParameter(typeof(Geometry));
        var member = typeof(Geometry).GetProperty(nameof(Geometry.Centroid))!;

        var result = _translator.Translate(instance, member, typeof(Point), null!);

        AssertFunctionTranslation(result, "ST_CENTROID", typeof(Point));
    }

    [Fact]
    public void Geometry_Dimension_Translates_To_ST_DIMENSION()
    {
        var instance = CreateGeometryParameter(typeof(Geometry));
        var member = typeof(Geometry).GetProperty(nameof(Geometry.Dimension))!;

        var result = _translator.Translate(instance, member, typeof(int), null!);

        AssertFunctionTranslation(result, "ST_DIMENSION", typeof(int));
    }

    [Fact]
    public void Geometry_GeometryType_Translates_To_ST_GEOMETRYTYPE()
    {
        var instance = CreateGeometryParameter(typeof(Geometry));
        var member = typeof(Geometry).GetProperty(nameof(Geometry.GeometryType))!;

        var result = _translator.Translate(instance, member, typeof(string), null!);

        AssertFunctionTranslation(result, "ST_GEOMETRYTYPE", typeof(string));
    }

    [Fact]
    public void Geometry_IsEmpty_Translates_To_ST_ISEMPTY()
    {
        var instance = CreateGeometryParameter(typeof(Geometry));
        var member = typeof(Geometry).GetProperty(nameof(Geometry.IsEmpty))!;

        var result = _translator.Translate(instance, member, typeof(bool), null!);

        AssertFunctionTranslation(result, "ST_ISEMPTY", typeof(bool));
    }

    [Fact]
    public void Geometry_Length_Translates_To_ST_LENGTH()
    {
        var instance = CreateGeometryParameter(typeof(Geometry));
        var member = typeof(Geometry).GetProperty(nameof(Geometry.Length))!;

        var result = _translator.Translate(instance, member, typeof(double), null!);

        AssertFunctionTranslation(result, "ST_LENGTH", typeof(double));
    }

    [Fact]
    public void Geometry_NumGeometries_Translates_To_ST_NUMGEOMETRIES()
    {
        var instance = CreateGeometryParameter(typeof(GeometryCollection));
        var member = typeof(Geometry).GetProperty(nameof(Geometry.NumGeometries))!;

        var result = _translator.Translate(instance, member, typeof(int), null!);

        AssertFunctionTranslation(result, "ST_NUMGEOMETRIES", typeof(int));
    }

    [Fact]
    public void Geometry_NumPoints_Translates_To_ST_NUMPOINTS()
    {
        var instance = CreateGeometryParameter(typeof(Geometry));
        var member = typeof(Geometry).GetProperty(nameof(Geometry.NumPoints))!;

        var result = _translator.Translate(instance, member, typeof(int), null!);

        AssertFunctionTranslation(result, "ST_NUMPOINTS", typeof(int));
    }

    #endregion

    #region Point Properties

    [Fact]
    public void Point_X_Translates_To_ST_X()
    {
        var instance = CreateGeometryParameter(typeof(Point));
        var member = typeof(Point).GetProperty(nameof(Point.X))!;

        var result = _translator.Translate(instance, member, typeof(double), null!);

        AssertFunctionTranslation(result, "ST_X", typeof(double));
    }

    [Fact]
    public void Point_Y_Translates_To_ST_Y()
    {
        var instance = CreateGeometryParameter(typeof(Point));
        var member = typeof(Point).GetProperty(nameof(Point.Y))!;

        var result = _translator.Translate(instance, member, typeof(double), null!);

        AssertFunctionTranslation(result, "ST_Y", typeof(double));
    }

    [Fact]
    public void Point_Z_Returns_Null_Not_Supported()
    {
        var instance = CreateGeometryParameter(typeof(Point));
        var member = typeof(Point).GetProperty(nameof(Point.Z))!;

        var result = _translator.Translate(instance, member, typeof(double), null!);

        Assert.Null(result);
    }

    [Fact]
    public void Point_M_Returns_Null_Not_Supported()
    {
        var instance = CreateGeometryParameter(typeof(Point));
        var member = typeof(Point).GetProperty(nameof(Point.M))!;

        var result = _translator.Translate(instance, member, typeof(double), null!);

        Assert.Null(result);
    }

    #endregion

    #region LineString Properties

    [Fact]
    public void LineString_StartPoint_Translates_To_ST_STARTPOINT()
    {
        var instance = CreateGeometryParameter(typeof(LineString));
        var member = typeof(LineString).GetProperty(nameof(LineString.StartPoint))!;

        var result = _translator.Translate(instance, member, typeof(Point), null!);

        AssertFunctionTranslation(result, "ST_STARTPOINT", typeof(Point));
    }

    [Fact]
    public void LineString_EndPoint_Translates_To_ST_ENDPOINT()
    {
        var instance = CreateGeometryParameter(typeof(LineString));
        var member = typeof(LineString).GetProperty(nameof(LineString.EndPoint))!;

        var result = _translator.Translate(instance, member, typeof(Point), null!);

        AssertFunctionTranslation(result, "ST_ENDPOINT", typeof(Point));
    }

    [Fact]
    public void LineString_IsClosed_Translates_To_ST_ISCLOSED()
    {
        var instance = CreateGeometryParameter(typeof(LineString));
        var member = typeof(LineString).GetProperty(nameof(LineString.IsClosed))!;

        var result = _translator.Translate(instance, member, typeof(bool), null!);

        AssertFunctionTranslation(result, "ST_ISCLOSED", typeof(bool));
    }

    [Fact]
    public void LineString_IsRing_Translates_To_ST_ISRING()
    {
        var instance = CreateGeometryParameter(typeof(LineString));
        var member = typeof(LineString).GetProperty(nameof(LineString.IsRing))!;

        var result = _translator.Translate(instance, member, typeof(bool), null!);

        AssertFunctionTranslation(result, "ST_ISRING", typeof(bool));
    }

    [Fact]
    public void LineString_NumPoints_Translates_To_ST_NUMPOINTS()
    {
        var instance = CreateGeometryParameter(typeof(LineString));
        var member = typeof(LineString).GetProperty(nameof(LineString.NumPoints))!;

        var result = _translator.Translate(instance, member, typeof(int), null!);

        AssertFunctionTranslation(result, "ST_NUMPOINTS", typeof(int));
    }

    #endregion

    #region Polygon Properties

    [Fact]
    public void Polygon_ExteriorRing_Translates_To_ST_EXTERIORRING()
    {
        var instance = CreateGeometryParameter(typeof(Polygon));
        var member = typeof(Polygon).GetProperty(nameof(Polygon.ExteriorRing))!;

        var result = _translator.Translate(instance, member, typeof(LineString), null!);

        AssertFunctionTranslation(result, "ST_EXTERIORRING", typeof(LineString));
    }

    [Fact]
    public void Polygon_NumInteriorRings_Translates_To_ST_NUMINTERIORRING()
    {
        var instance = CreateGeometryParameter(typeof(Polygon));
        var member = typeof(Polygon).GetProperty(nameof(Polygon.NumInteriorRings))!;

        var result = _translator.Translate(instance, member, typeof(int), null!);

        AssertFunctionTranslation(result, "ST_NUMINTERIORRING", typeof(int));
    }

    #endregion

    #region Null and Invalid Input Tests

    [Fact]
    public void Translate_NullInstance_ReturnsNull()
    {
        var member = typeof(Geometry).GetProperty(nameof(Geometry.Area))!;

        var result = _translator.Translate(null, member, typeof(double), null!);

        Assert.Null(result);
    }

    [Fact]
    public void Translate_NonGeometryType_ReturnsNull()
    {
        var stringTypeMapping = _typeMappingSource.FindMapping(typeof(string));
        var instance = new SqlParameterExpression("p0", typeof(string), stringTypeMapping);
        var member = typeof(string).GetProperty(nameof(string.Length))!;

        var result = _translator.Translate(instance, member, typeof(int), null!);

        Assert.Null(result);
    }

    #endregion

    #region Helper Methods

    private SqlParameterExpression CreateGeometryParameter(Type geometryType)
    {
        var typeMapping = _ntsPlugin.FindMapping(new RelationalTypeMappingInfo(geometryType));
        return new SqlParameterExpression("p0", geometryType, typeMapping);
    }

    private static void AssertFunctionTranslation(SqlExpression? result, string expectedFunctionName, Type expectedReturnType)
    {
        Assert.NotNull(result);
        var function = Assert.IsType<SqlFunctionExpression>(result);
        Assert.Equal(expectedFunctionName, function.Name);
        Assert.Single(function.Arguments!);
        Assert.Equal(expectedReturnType, function.Type);
    }

    #endregion
}
