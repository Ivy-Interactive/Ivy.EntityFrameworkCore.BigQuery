using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Xunit;
using Ivy.EntityFrameworkCore.BigQuery.Query.Internal;
using Ivy.EntityFrameworkCore.BigQuery.Storage.Internal;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.Json;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using System.Reflection;

namespace Ivy.EntityFrameworkCore.BigQuery.Tests.Query;

public class MathFunctionsTranslationTests
{
    private readonly BigQueryMathMethodTranslator _translator;
    private readonly BigQuerySqlExpressionFactory _factory;
    private readonly IRelationalTypeMappingSource _typeMappingSource;

    public MathFunctionsTranslationTests()
    {
        _typeMappingSource = new BigQueryTypeMappingSource(
            new TypeMappingSourceDependencies(
                new ValueConverterSelector(new ValueConverterSelectorDependencies()),
                new JsonValueReaderWriterSource(new JsonValueReaderWriterSourceDependencies()),
                []
            ),
            new RelationalTypeMappingSourceDependencies([]));

        var model = new Model();
        var dependencies = new SqlExpressionFactoryDependencies(model, _typeMappingSource);
        _factory = new BigQuerySqlExpressionFactory(dependencies);

        _translator = new BigQueryMathMethodTranslator(_factory, _typeMappingSource);
    }

    #region Absolute Value Tests

    [Fact]
    public void Math_Abs_decimal_translates_to_ABS()
    {
        var method = typeof(Math).GetMethod(nameof(Math.Abs), [typeof(decimal)])!;
        var argument = CreateSqlParameter(typeof(decimal));

        var result = _translator.Translate(null, method, [argument], null!);

        AssertFunctionTranslation(result, "ABS");
    }

    [Fact]
    public void Math_Abs_double_translates_to_ABS()
    {
        var method = typeof(Math).GetMethod(nameof(Math.Abs), [typeof(double)])!;
        var argument = CreateSqlParameter(typeof(double));

        var result = _translator.Translate(null, method, [argument], null!);

        AssertFunctionTranslation(result, "ABS");
    }

    [Fact]
    public void Math_Abs_int_translates_to_ABS()
    {
        var method = typeof(Math).GetMethod(nameof(Math.Abs), [typeof(int)])!;
        var argument = CreateSqlParameter(typeof(int));

        var result = _translator.Translate(null, method, [argument], null!);

        AssertFunctionTranslation(result, "ABS");
    }

    [Fact]
    public void MathF_Abs_float_translates_to_ABS()
    {
        var method = typeof(MathF).GetMethod(nameof(MathF.Abs), [typeof(float)])!;
        var argument = CreateSqlParameter(typeof(float));

        var result = _translator.Translate(null, method, [argument], null!);

        AssertFunctionTranslation(result, "ABS");
    }

    #endregion

    #region Ceiling and Floor Tests

    [Fact]
    public void Math_Ceiling_decimal_translates_to_CEIL()
    {
        var method = typeof(Math).GetMethod(nameof(Math.Ceiling), [typeof(decimal)])!;
        var argument = CreateSqlParameter(typeof(decimal));

        var result = _translator.Translate(null, method, [argument], null!);

        AssertFunctionTranslation(result, "CEIL");
    }

    [Fact]
    public void Math_Ceiling_double_translates_to_CEIL()
    {
        var method = typeof(Math).GetMethod(nameof(Math.Ceiling), [typeof(double)])!;
        var argument = CreateSqlParameter(typeof(double));

        var result = _translator.Translate(null, method, [argument], null!);

        AssertFunctionTranslation(result, "CEIL");
    }

    [Fact]
    public void Math_Floor_decimal_translates_to_FLOOR()
    {
        var method = typeof(Math).GetMethod(nameof(Math.Floor), [typeof(decimal)])!;
        var argument = CreateSqlParameter(typeof(decimal));

        var result = _translator.Translate(null, method, [argument], null!);

        AssertFunctionTranslation(result, "FLOOR");
    }

    [Fact]
    public void Math_Floor_double_translates_to_FLOOR()
    {
        var method = typeof(Math).GetMethod(nameof(Math.Floor), [typeof(double)])!;
        var argument = CreateSqlParameter(typeof(double));

        var result = _translator.Translate(null, method, [argument], null!);

        AssertFunctionTranslation(result, "FLOOR");
    }

    #endregion

    #region Round and Truncate Tests

    [Fact]
    public void Math_Round_decimal_translates_to_ROUND()
    {
        var method = typeof(Math).GetMethod(nameof(Math.Round), [typeof(decimal)])!;
        var argument = CreateSqlParameter(typeof(decimal));

        var result = _translator.Translate(null, method, [argument], null!);

        AssertFunctionTranslation(result, "ROUND");
    }

    [Fact]
    public void Math_Round_decimal_with_precision_translates_to_ROUND()
    {
        var method = typeof(Math).GetMethod(nameof(Math.Round), [typeof(decimal), typeof(int)])!;
        var argument1 = CreateSqlParameter(typeof(decimal));
        var argument2 = CreateSqlParameter(typeof(int));

        var result = _translator.Translate(null, method, [argument1, argument2], null!);

        var function = Assert.IsType<SqlFunctionExpression>(result);
        Assert.Equal("ROUND", function.Name);
        Assert.Equal(2, function.Arguments.Count);
    }

    [Fact]
    public void Math_Truncate_decimal_translates_to_TRUNC()
    {
        var method = typeof(Math).GetMethod(nameof(Math.Truncate), [typeof(decimal)])!;
        var argument = CreateSqlParameter(typeof(decimal));

        var result = _translator.Translate(null, method, [argument], null!);

        AssertFunctionTranslation(result, "TRUNC");
    }

    [Fact]
    public void Math_Truncate_double_translates_to_TRUNC()
    {
        var method = typeof(Math).GetMethod(nameof(Math.Truncate), [typeof(double)])!;
        var argument = CreateSqlParameter(typeof(double));

        var result = _translator.Translate(null, method, [argument], null!);

        AssertFunctionTranslation(result, "TRUNC");
    }

    #endregion

    #region Power and Root Tests

    [Fact]
    public void Math_Pow_translates_to_POW()
    {
        var method = typeof(Math).GetMethod(nameof(Math.Pow), [typeof(double), typeof(double)])!;
        var argument1 = CreateSqlParameter(typeof(double));
        var argument2 = CreateSqlParameter(typeof(double));

        var result = _translator.Translate(null, method, [argument1, argument2], null!);

        var function = Assert.IsType<SqlFunctionExpression>(result);
        Assert.Equal("POW", function.Name);
        Assert.Equal(2, function.Arguments.Count);
    }

    [Fact]
    public void Math_Sqrt_translates_to_SQRT()
    {
        var method = typeof(Math).GetMethod(nameof(Math.Sqrt), [typeof(double)])!;
        var argument = CreateSqlParameter(typeof(double));

        var result = _translator.Translate(null, method, [argument], null!);

        AssertFunctionTranslation(result, "SQRT");
    }

    #endregion

    #region Exponential and Logarithm Tests

    [Fact]
    public void Math_Exp_translates_to_EXP()
    {
        var method = typeof(Math).GetMethod(nameof(Math.Exp), [typeof(double)])!;
        var argument = CreateSqlParameter(typeof(double));

        var result = _translator.Translate(null, method, [argument], null!);

        AssertFunctionTranslation(result, "EXP");
    }

    [Fact]
    public void Math_Log_translates_to_LN()
    {
        var method = typeof(Math).GetMethod(nameof(Math.Log), [typeof(double)])!;
        var argument = CreateSqlParameter(typeof(double));

        var result = _translator.Translate(null, method, [argument], null!);

        AssertFunctionTranslation(result, "LN");
    }

    [Fact]
    public void Math_Log10_translates_to_LOG10()
    {
        var method = typeof(Math).GetMethod(nameof(Math.Log10), [typeof(double)])!;
        var argument = CreateSqlParameter(typeof(double));

        var result = _translator.Translate(null, method, [argument], null!);

        AssertFunctionTranslation(result, "LOG10");
    }

    [Fact]
    public void Math_Log_with_base_translates_to_LOG()
    {
        var method = typeof(Math).GetMethod(nameof(Math.Log), [typeof(double), typeof(double)])!;
        var argument1 = CreateSqlParameter(typeof(double));
        var argument2 = CreateSqlParameter(typeof(double));

        var result = _translator.Translate(null, method, [argument1, argument2], null!);

        var function = Assert.IsType<SqlFunctionExpression>(result);
        Assert.Equal("LOG", function.Name);
        Assert.Equal(2, function.Arguments.Count);
    }

    #endregion

    #region Trigonometric Tests

    [Fact]
    public void Math_Sin_translates_to_SIN()
    {
        var method = typeof(Math).GetMethod(nameof(Math.Sin), [typeof(double)])!;
        var argument = CreateSqlParameter(typeof(double));

        var result = _translator.Translate(null, method, [argument], null!);

        AssertFunctionTranslation(result, "SIN");
    }

    [Fact]
    public void Math_Cos_translates_to_COS()
    {
        var method = typeof(Math).GetMethod(nameof(Math.Cos), [typeof(double)])!;
        var argument = CreateSqlParameter(typeof(double));

        var result = _translator.Translate(null, method, [argument], null!);

        AssertFunctionTranslation(result, "COS");
    }

    [Fact]
    public void Math_Tan_translates_to_TAN()
    {
        var method = typeof(Math).GetMethod(nameof(Math.Tan), [typeof(double)])!;
        var argument = CreateSqlParameter(typeof(double));

        var result = _translator.Translate(null, method, [argument], null!);

        AssertFunctionTranslation(result, "TAN");
    }

    [Fact]
    public void Math_Asin_translates_to_ASIN()
    {
        var method = typeof(Math).GetMethod(nameof(Math.Asin), [typeof(double)])!;
        var argument = CreateSqlParameter(typeof(double));

        var result = _translator.Translate(null, method, [argument], null!);

        AssertFunctionTranslation(result, "ASIN");
    }

    [Fact]
    public void Math_Acos_translates_to_ACOS()
    {
        var method = typeof(Math).GetMethod(nameof(Math.Acos), [typeof(double)])!;
        var argument = CreateSqlParameter(typeof(double));

        var result = _translator.Translate(null, method, [argument], null!);

        AssertFunctionTranslation(result, "ACOS");
    }

    [Fact]
    public void Math_Atan_translates_to_ATAN()
    {
        var method = typeof(Math).GetMethod(nameof(Math.Atan), [typeof(double)])!;
        var argument = CreateSqlParameter(typeof(double));

        var result = _translator.Translate(null, method, [argument], null!);

        AssertFunctionTranslation(result, "ATAN");
    }

    [Fact]
    public void Math_Atan2_translates_to_ATAN2()
    {
        var method = typeof(Math).GetMethod(nameof(Math.Atan2), [typeof(double), typeof(double)])!;
        var argument1 = CreateSqlParameter(typeof(double));
        var argument2 = CreateSqlParameter(typeof(double));

        var result = _translator.Translate(null, method, [argument1, argument2], null!);

        var function = Assert.IsType<SqlFunctionExpression>(result);
        Assert.Equal("ATAN2", function.Name);
        Assert.Equal(2, function.Arguments.Count);
    }

    #endregion

    #region Hyperbolic Tests

    [Fact]
    public void Math_Sinh_translates_to_SINH()
    {
        var method = typeof(Math).GetMethod(nameof(Math.Sinh), [typeof(double)])!;
        var argument = CreateSqlParameter(typeof(double));

        var result = _translator.Translate(null, method, [argument], null!);

        AssertFunctionTranslation(result, "SINH");
    }

    [Fact]
    public void Math_Cosh_translates_to_COSH()
    {
        var method = typeof(Math).GetMethod(nameof(Math.Cosh), [typeof(double)])!;
        var argument = CreateSqlParameter(typeof(double));

        var result = _translator.Translate(null, method, [argument], null!);

        AssertFunctionTranslation(result, "COSH");
    }

    [Fact]
    public void Math_Tanh_translates_to_TANH()
    {
        var method = typeof(Math).GetMethod(nameof(Math.Tanh), [typeof(double)])!;
        var argument = CreateSqlParameter(typeof(double));

        var result = _translator.Translate(null, method, [argument], null!);

        AssertFunctionTranslation(result, "TANH");
    }

    [Fact]
    public void Math_Asinh_translates_to_ASINH()
    {
        var method = typeof(Math).GetMethod(nameof(Math.Asinh), [typeof(double)])!;
        var argument = CreateSqlParameter(typeof(double));

        var result = _translator.Translate(null, method, [argument], null!);

        AssertFunctionTranslation(result, "ASINH");
    }

    [Fact]
    public void Math_Acosh_translates_to_ACOSH()
    {
        var method = typeof(Math).GetMethod(nameof(Math.Acosh), [typeof(double)])!;
        var argument = CreateSqlParameter(typeof(double));

        var result = _translator.Translate(null, method, [argument], null!);

        AssertFunctionTranslation(result, "ACOSH");
    }

    [Fact]
    public void Math_Atanh_translates_to_ATANH()
    {
        var method = typeof(Math).GetMethod(nameof(Math.Atanh), [typeof(double)])!;
        var argument = CreateSqlParameter(typeof(double));

        var result = _translator.Translate(null, method, [argument], null!);

        AssertFunctionTranslation(result, "ATANH");
    }

    #endregion

    #region Sign Test

    [Fact]
    public void Math_Sign_decimal_translates_to_SIGN()
    {
        var method = typeof(Math).GetMethod(nameof(Math.Sign), [typeof(decimal)])!;
        var argument = CreateSqlParameter(typeof(decimal));

        var result = _translator.Translate(null, method, [argument], null!);

        AssertFunctionTranslation(result, "SIGN");
    }

    [Fact]
    public void Math_Sign_double_translates_to_SIGN()
    {
        var method = typeof(Math).GetMethod(nameof(Math.Sign), [typeof(double)])!;
        var argument = CreateSqlParameter(typeof(double));

        var result = _translator.Translate(null, method, [argument], null!);

        AssertFunctionTranslation(result, "SIGN");
    }

    [Fact]
    public void Math_Sign_int_translates_to_SIGN()
    {
        var method = typeof(Math).GetMethod(nameof(Math.Sign), [typeof(int)])!;
        var argument = CreateSqlParameter(typeof(int));

        var result = _translator.Translate(null, method, [argument], null!);

        AssertFunctionTranslation(result, "SIGN");
    }

    #endregion

    #region MathF Tests

    [Fact]
    public void MathF_Ceiling_translates_to_CEIL()
    {
        var method = typeof(MathF).GetMethod(nameof(MathF.Ceiling), [typeof(float)])!;
        var argument = CreateSqlParameter(typeof(float));

        var result = _translator.Translate(null, method, [argument], null!);

        AssertFunctionTranslation(result, "CEIL");
    }

    [Fact]
    public void MathF_Sin_translates_to_SIN()
    {
        var method = typeof(MathF).GetMethod(nameof(MathF.Sin), [typeof(float)])!;
        var argument = CreateSqlParameter(typeof(float));

        var result = _translator.Translate(null, method, [argument], null!);

        AssertFunctionTranslation(result, "SIN");
    }

    [Fact]
    public void MathF_Sqrt_translates_to_SQRT()
    {
        var method = typeof(MathF).GetMethod(nameof(MathF.Sqrt), [typeof(float)])!;
        var argument = CreateSqlParameter(typeof(float));

        var result = _translator.Translate(null, method, [argument], null!);

        AssertFunctionTranslation(result, "SQRT");
    }

    #endregion

    #region Helper Methods

    private SqlParameterExpression CreateSqlParameter(Type type)
    {
        var typeMapping = _typeMappingSource.FindMapping(type);
        return new SqlParameterExpression("p0", type, typeMapping);
    }

    private void AssertFunctionTranslation(SqlExpression? result, string expectedFunctionName)
    {
        Assert.NotNull(result);
        var function = Assert.IsType<SqlFunctionExpression>(result);
        Assert.Equal(expectedFunctionName, function.Name);
        Assert.Single(function.Arguments);
    }

    #endregion
}
