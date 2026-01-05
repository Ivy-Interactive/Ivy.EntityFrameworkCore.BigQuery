using Ivy.EntityFrameworkCore.BigQuery.Query.Internal;
using Ivy.EntityFrameworkCore.BigQuery.Storage.Internal;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.Json;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using System.Linq.Expressions;
using System.Reflection;
using Xunit;

#pragma warning disable EF1001 // Internal EF Core API usage.

namespace Ivy.EntityFrameworkCore.BigQuery.Tests.Query;

public class BigQueryArrayMethodTranslatorTest
{
    private readonly BigQueryArrayMethodTranslator _translator;
    private readonly BigQuerySqlExpressionFactory _sqlExpressionFactory;
    private readonly IRelationalTypeMappingSource _typeMappingSource;

    public BigQueryArrayMethodTranslatorTest()
    {
        _typeMappingSource = new BigQueryTypeMappingSource(
            new TypeMappingSourceDependencies(
                new ValueConverterSelector(new ValueConverterSelectorDependencies()),
                new JsonValueReaderWriterSource(new JsonValueReaderWriterSourceDependencies()),
                []),
            new RelationalTypeMappingSourceDependencies([]));

        var model = new Microsoft.EntityFrameworkCore.Metadata.Internal.Model();
        var dependencies = new SqlExpressionFactoryDependencies(
            model,
            _typeMappingSource);

        _sqlExpressionFactory = new BigQuerySqlExpressionFactory(dependencies);
        _translator = new BigQueryArrayMethodTranslator(_sqlExpressionFactory);
    }

    [Fact]
    public void Translate_Array_Length_Returns_ARRAY_LENGTH()
    {
        var arrayTypeMapping = _typeMappingSource.FindMapping(typeof(int[]));
        var array = new SqlParameterExpression("p0", typeof(int[]), arrayTypeMapping);
        var method = typeof(Array).GetProperty(nameof(Array.Length))!.GetMethod!;

        var result = _translator.Translate(
            array,
            method,
            [],
            null!);

        Assert.NotNull(result);
        var functionExpression = Assert.IsType<SqlFunctionExpression>(result);
        Assert.Equal("ARRAY_LENGTH", functionExpression.Name);
        Assert.NotNull(functionExpression.Arguments);
        Assert.Single(functionExpression.Arguments);
    }

    [Fact]
    public void Translate_Enumerable_Count_Returns_ARRAY_LENGTH()
    {
        var arrayTypeMapping = _typeMappingSource.FindMapping(typeof(int[]));
        var array = new SqlParameterExpression("p0", typeof(int[]), arrayTypeMapping);
        var method = typeof(Enumerable).GetMethods()
            .Single(m => m.Name == nameof(Enumerable.Count) && m.GetParameters().Length == 1)
            .MakeGenericMethod(typeof(int));

        var result = _translator.Translate(
            array,
            method,
            [array],
            null!);

        Assert.NotNull(result);
        var functionExpression = Assert.IsType<SqlFunctionExpression>(result);
        Assert.Equal("ARRAY_LENGTH", functionExpression.Name);
    }

    [Fact]
    public void Translate_ElementAt_Returns_ArrayIndexExpression()
    {
        var arrayTypeMapping = _typeMappingSource.FindMapping(typeof(int[]));
        var array = new SqlParameterExpression("p0", typeof(int[]), arrayTypeMapping);
        var index = new SqlConstantExpression(2, _typeMappingSource.FindMapping(typeof(int)));
        var method = typeof(Enumerable).GetRuntimeMethods()
            .Single(m => m.Name == nameof(Enumerable.ElementAt) &&
                         m.GetParameters().Length == 2 &&
                         m.IsGenericMethod &&
                         m.GetParameters()[1].ParameterType == typeof(int))
            .MakeGenericMethod(typeof(int));

        var result = _translator.Translate(
            array,
            method,
            [array, index],
            null!);

        Assert.NotNull(result);
        var arrayIndexExpression = Assert.IsType<BigQueryArrayIndexExpression>(result);
        Assert.Same(array, arrayIndexExpression.Array);
        Assert.Same(index, arrayIndexExpression.Index);
    }

    [Fact]
    public void Translate_First_Returns_ArrayIndexExpression_WithZeroIndex()
    {
        var arrayTypeMapping = _typeMappingSource.FindMapping(typeof(int[]));
        var array = new SqlParameterExpression("p0", typeof(int[]), arrayTypeMapping);
        var method = typeof(Enumerable).GetMethods()
            .Single(m => m.Name == nameof(Enumerable.First) && m.GetParameters().Length == 1)
            .MakeGenericMethod(typeof(int));

        var result = _translator.Translate(
            array,
            method,
            [array],
            null!);

        Assert.NotNull(result);
        var arrayIndexExpression = Assert.IsType<BigQueryArrayIndexExpression>(result);
        Assert.Same(array, arrayIndexExpression.Array);
        var indexConstant = Assert.IsType<SqlConstantExpression>(arrayIndexExpression.Index);
        Assert.Equal(0, indexConstant.Value);
    }

    [Fact]
    public void Translate_NonArrayType_ReturnsNull()
    {
        var stringTypeMapping = _typeMappingSource.FindMapping(typeof(string));
        var stringParam = new SqlParameterExpression("p0", typeof(string), stringTypeMapping);
        var method = typeof(string).GetProperty(nameof(string.Length))!.GetMethod!;

        var result = _translator.Translate(
            stringParam,
            method,
            [],
            null!);

        Assert.Null(result);
    }

    [Fact]
    public void Translate_ListIndexer_Returns_ArrayIndexExpression()
    {
        var arrayTypeMapping = _typeMappingSource.FindMapping(typeof(List<string>));
        var array = new SqlParameterExpression("p0", typeof(List<string>), arrayTypeMapping);
        var index = new SqlConstantExpression(1, _typeMappingSource.FindMapping(typeof(int)));
        var method = typeof(List<string>).GetProperty("Item")!.GetMethod!;

        var result = _translator.Translate(
            array,
            method,
            [index],
            null!);

        Assert.NotNull(result);
        var arrayIndexExpression = Assert.IsType<BigQueryArrayIndexExpression>(result);
        Assert.Same(array, arrayIndexExpression.Array);
        Assert.Same(index, arrayIndexExpression.Index);
    }
}