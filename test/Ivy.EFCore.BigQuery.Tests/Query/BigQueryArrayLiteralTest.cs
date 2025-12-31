using Ivy.EntityFrameworkCore.BigQuery.Query.Internal;
using Ivy.EntityFrameworkCore.BigQuery.Storage.Internal;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.Json;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using Xunit;

#pragma warning disable EF1001 // Internal EF Core API usage.

namespace Ivy.EntityFrameworkCore.BigQuery.Tests.Query;

public class BigQueryArrayLiteralTest
{
    private readonly BigQuerySqlExpressionFactory _sqlExpressionFactory;
    private readonly IRelationalTypeMappingSource _typeMappingSource;

    public BigQueryArrayLiteralTest()
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
    }

    [Fact]
    public void ArrayLiteral_AllConstants_ReturnsConstantExpression()
    {
        var intMapping = _typeMappingSource.FindMapping(typeof(int));
        var arrayMapping = _typeMappingSource.FindMapping(typeof(int[]));

        var elements = new List<SqlExpression>
        {
            _sqlExpressionFactory.Constant(1, intMapping),
            _sqlExpressionFactory.Constant(2, intMapping),
            _sqlExpressionFactory.Constant(3, intMapping)
        };

        var result = _sqlExpressionFactory.ArrayLiteral(
            elements,
            typeof(int[]),
            typeof(int),
            arrayMapping,
            intMapping);

        var constantExpression = Assert.IsType<SqlConstantExpression>(result);
        Assert.NotNull(constantExpression.Value);
        var array = Assert.IsType<int[]>(constantExpression.Value);
        Assert.Equal(new[] { 1, 2, 3 }, array);
    }

    [Fact]
    public void ArrayLiteral_MixedExpressions_ReturnsArrayLiteralExpression()
    {
        var intMapping = _typeMappingSource.FindMapping(typeof(int));
        var arrayMapping = _typeMappingSource.FindMapping(typeof(int[]));

        var elements = new List<SqlExpression>
        {
            _sqlExpressionFactory.Constant(1, intMapping),
            new SqlParameterExpression("p0", typeof(int), intMapping),
            _sqlExpressionFactory.Constant(3, intMapping)
        };

        var result = _sqlExpressionFactory.ArrayLiteral(
            elements,
            typeof(int[]),
            typeof(int),
            arrayMapping,
            intMapping);

        var arrayLiteral = Assert.IsType<BigQueryArrayLiteralExpression>(result);
        Assert.Equal(3, arrayLiteral.Elements.Count);
        Assert.Equal(typeof(int), arrayLiteral.ElementType);
    }

    [Fact]
    public void ArrayLiteral_EmptyArray_ReturnsConstantExpression()
    {
        var intMapping = _typeMappingSource.FindMapping(typeof(int));
        var arrayMapping = _typeMappingSource.FindMapping(typeof(int[]));

        var elements = new List<SqlExpression>();

        var result = _sqlExpressionFactory.ArrayLiteral(
            elements,
            typeof(int[]),
            typeof(int),
            arrayMapping,
            intMapping);

        var constantExpression = Assert.IsType<SqlConstantExpression>(result);
        Assert.NotNull(constantExpression.Value);
        var array = Assert.IsType<int[]>(constantExpression.Value);
        Assert.Empty(array);
    }

    [Fact]
    public void ArrayLiteral_StringConstants_ReturnsConstantExpression()
    {
        var stringMapping = _typeMappingSource.FindMapping(typeof(string));
        var arrayMapping = _typeMappingSource.FindMapping(typeof(string[]));

        var elements = new List<SqlExpression>
        {
            _sqlExpressionFactory.Constant("hello", stringMapping),
            _sqlExpressionFactory.Constant("world", stringMapping)
        };

        var result = _sqlExpressionFactory.ArrayLiteral(
            elements,
            typeof(string[]),
            typeof(string),
            arrayMapping,
            stringMapping);

        var constantExpression = Assert.IsType<SqlConstantExpression>(result);
        Assert.NotNull(constantExpression.Value);
        var array = Assert.IsType<string[]>(constantExpression.Value);
        Assert.Equal(new[] { "hello", "world" }, array);
    }

    [Fact]
    public void ArrayLiteralExpression_Print_GeneratesCorrectOutput()
    {
        var intMapping = _typeMappingSource.FindMapping(typeof(int));
        var arrayMapping = _typeMappingSource.FindMapping(typeof(int[]));

        var elements = new List<SqlExpression>
        {
            _sqlExpressionFactory.Constant(1, intMapping),
            new SqlParameterExpression("p0", typeof(int), intMapping)
        };

        var arrayLiteral = new BigQueryArrayLiteralExpression(
            elements,
            typeof(int[]),
            typeof(int),
            arrayMapping,
            intMapping);

        var printer = new ExpressionPrinter();
        printer.Visit(arrayLiteral);
        var output = printer.ToString();

        Assert.Contains("ARRAY<INT64>[", output);
    }

    [Fact]
    public void ArrayLiteralExpression_VisitChildren_UpdatesElements()
    {

        var intMapping = _typeMappingSource.FindMapping(typeof(int));
        var arrayMapping = _typeMappingSource.FindMapping(typeof(int[]));

        var elements = new List<SqlExpression>
        {
            _sqlExpressionFactory.Constant(1, intMapping),
            _sqlExpressionFactory.Constant(2, intMapping)
        };

        var arrayLiteral = new BigQueryArrayLiteralExpression(
            elements,
            typeof(int[]),
            typeof(int),
            arrayMapping,
            intMapping);

        var visitor = new ReplacingExpressionVisitor(
            new[] { elements[0] },
            new[] { _sqlExpressionFactory.Constant(10, intMapping) });


        var result = (BigQueryArrayLiteralExpression)visitor.Visit(arrayLiteral);


        Assert.NotSame(arrayLiteral, result);
        var firstElement = Assert.IsType<SqlConstantExpression>(result.Elements[0]);
        Assert.Equal(10, firstElement.Value);
    }
}
