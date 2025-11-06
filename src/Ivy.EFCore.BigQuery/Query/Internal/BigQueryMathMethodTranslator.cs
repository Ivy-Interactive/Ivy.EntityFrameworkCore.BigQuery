using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
using System.Reflection;
using ExpressionExtensions = Microsoft.EntityFrameworkCore.Query.ExpressionExtensions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal;

/// <summary>
///     Provides translation services for static <see cref="Math" /> and <see cref="MathF" /> methods to BigQuery SQL functions.
/// </summary>
/// <remarks>
///     See: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions
/// </remarks>
public class BigQueryMathMethodTranslator : IMethodCallTranslator
{
    private static readonly Dictionary<MethodInfo, string> SupportedMethodTranslations = new()
    {
        // Absolute value
        { typeof(Math).GetRuntimeMethod(nameof(Math.Abs), [typeof(decimal)])!, "ABS" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Abs), [typeof(double)])!, "ABS" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Abs), [typeof(float)])!, "ABS" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Abs), [typeof(int)])!, "ABS" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Abs), [typeof(long)])!, "ABS" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Abs), [typeof(sbyte)])!, "ABS" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Abs), [typeof(short)])!, "ABS" },
        { typeof(MathF).GetRuntimeMethod(nameof(MathF.Abs), [typeof(float)])!, "ABS" },

        // Ceiling and Floor
        { typeof(Math).GetRuntimeMethod(nameof(Math.Ceiling), [typeof(decimal)])!, "CEIL" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Ceiling), [typeof(double)])!, "CEIL" },
        { typeof(MathF).GetRuntimeMethod(nameof(MathF.Ceiling), [typeof(float)])!, "CEIL" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Floor), [typeof(decimal)])!, "FLOOR" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Floor), [typeof(double)])!, "FLOOR" },
        { typeof(MathF).GetRuntimeMethod(nameof(MathF.Floor), [typeof(float)])!, "FLOOR" },

        // Power and roots
        { typeof(Math).GetRuntimeMethod(nameof(Math.Pow), [typeof(double), typeof(double)])!, "POW" },
        { typeof(MathF).GetRuntimeMethod(nameof(MathF.Pow), [typeof(float), typeof(float)])!, "POW" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Sqrt), [typeof(double)])!, "SQRT" },
        { typeof(MathF).GetRuntimeMethod(nameof(MathF.Sqrt), [typeof(float)])!, "SQRT" },

        // Exponential and logarithms
        { typeof(Math).GetRuntimeMethod(nameof(Math.Exp), [typeof(double)])!, "EXP" },
        { typeof(MathF).GetRuntimeMethod(nameof(MathF.Exp), [typeof(float)])!, "EXP" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Log), [typeof(double)])!, "LN" },
        { typeof(MathF).GetRuntimeMethod(nameof(MathF.Log), [typeof(float)])!, "LN" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Log10), [typeof(double)])!, "LOG10" },
        { typeof(MathF).GetRuntimeMethod(nameof(MathF.Log10), [typeof(float)])!, "LOG10" },

        // Trigonometric functions
        { typeof(Math).GetRuntimeMethod(nameof(Math.Acos), [typeof(double)])!, "ACOS" },
        { typeof(MathF).GetRuntimeMethod(nameof(MathF.Acos), [typeof(float)])!, "ACOS" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Asin), [typeof(double)])!, "ASIN" },
        { typeof(MathF).GetRuntimeMethod(nameof(MathF.Asin), [typeof(float)])!, "ASIN" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Atan), [typeof(double)])!, "ATAN" },
        { typeof(MathF).GetRuntimeMethod(nameof(MathF.Atan), [typeof(float)])!, "ATAN" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Atan2), [typeof(double), typeof(double)])!, "ATAN2" },
        { typeof(MathF).GetRuntimeMethod(nameof(MathF.Atan2), [typeof(float), typeof(float)])!, "ATAN2" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Cos), [typeof(double)])!, "COS" },
        { typeof(MathF).GetRuntimeMethod(nameof(MathF.Cos), [typeof(float)])!, "COS" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Sin), [typeof(double)])!, "SIN" },
        { typeof(MathF).GetRuntimeMethod(nameof(MathF.Sin), [typeof(float)])!, "SIN" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Tan), [typeof(double)])!, "TAN" },
        { typeof(MathF).GetRuntimeMethod(nameof(MathF.Tan), [typeof(float)])!, "TAN" },

        // Hyperbolic functions
        { typeof(Math).GetRuntimeMethod(nameof(Math.Acosh), [typeof(double)])!, "ACOSH" },
        { typeof(MathF).GetRuntimeMethod(nameof(MathF.Acosh), [typeof(float)])!, "ACOSH" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Asinh), [typeof(double)])!, "ASINH" },
        { typeof(MathF).GetRuntimeMethod(nameof(MathF.Asinh), [typeof(float)])!, "ASINH" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Atanh), [typeof(double)])!, "ATANH" },
        { typeof(MathF).GetRuntimeMethod(nameof(MathF.Atanh), [typeof(float)])!, "ATANH" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Cosh), [typeof(double)])!, "COSH" },
        { typeof(MathF).GetRuntimeMethod(nameof(MathF.Cosh), [typeof(float)])!, "COSH" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Sinh), [typeof(double)])!, "SINH" },
        { typeof(MathF).GetRuntimeMethod(nameof(MathF.Sinh), [typeof(float)])!, "SINH" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Tanh), [typeof(double)])!, "TANH" },
        { typeof(MathF).GetRuntimeMethod(nameof(MathF.Tanh), [typeof(float)])!, "TANH" },

        // Sign function
        { typeof(Math).GetRuntimeMethod(nameof(Math.Sign), [typeof(decimal)])!, "SIGN" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Sign), [typeof(double)])!, "SIGN" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Sign), [typeof(float)])!, "SIGN" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Sign), [typeof(int)])!, "SIGN" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Sign), [typeof(long)])!, "SIGN" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Sign), [typeof(sbyte)])!, "SIGN" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Sign), [typeof(short)])!, "SIGN" },
        { typeof(MathF).GetRuntimeMethod(nameof(MathF.Sign), [typeof(float)])!, "SIGN" },
    };

    private static readonly IEnumerable<MethodInfo> TruncateMethodInfos =
    [
        typeof(Math).GetRuntimeMethod(nameof(Math.Truncate), [typeof(decimal)])!,
        typeof(Math).GetRuntimeMethod(nameof(Math.Truncate), [typeof(double)])!,
        typeof(MathF).GetRuntimeMethod(nameof(MathF.Truncate), [typeof(float)])!
    ];

    private static readonly IEnumerable<MethodInfo> RoundMethodInfos =
    [
        typeof(Math).GetRuntimeMethod(nameof(Math.Round), [typeof(decimal)])!,
        typeof(Math).GetRuntimeMethod(nameof(Math.Round), [typeof(double)])!,
        typeof(MathF).GetRuntimeMethod(nameof(MathF.Round), [typeof(float)])!
    ];

    private static readonly IEnumerable<MethodInfo> RoundWithPrecisionMethodInfos =
    [
        typeof(Math).GetRuntimeMethod(nameof(Math.Round), [typeof(decimal), typeof(int)])!,
        typeof(Math).GetRuntimeMethod(nameof(Math.Round), [typeof(double), typeof(int)])!,
        typeof(MathF).GetRuntimeMethod(nameof(MathF.Round), [typeof(float), typeof(int)])!
    ];

    private static readonly IEnumerable<MethodInfo> LogWithBaseMethodInfos =
    [
        typeof(Math).GetRuntimeMethod(nameof(Math.Log), [typeof(double), typeof(double)])!,
        typeof(MathF).GetRuntimeMethod(nameof(MathF.Log), [typeof(float), typeof(float)])!
    ];

    private readonly ISqlExpressionFactory _sqlExpressionFactory;
    private readonly IRelationalTypeMappingSource _typeMappingSource;

    public BigQueryMathMethodTranslator(
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
        if (SupportedMethodTranslations.TryGetValue(method, out var sqlFunctionName))
        {
            var typeMapping = arguments.Count == 1
                ? ExpressionExtensions.InferTypeMapping(arguments[0])
                : ExpressionExtensions.InferTypeMapping(arguments[0], arguments[1]);

            var newArguments = new SqlExpression[arguments.Count];
            newArguments[0] = _sqlExpressionFactory.ApplyTypeMapping(arguments[0], typeMapping);

            if (arguments.Count == 2)
            {
                newArguments[1] = _sqlExpressionFactory.ApplyTypeMapping(arguments[1], typeMapping);
            }

            return _sqlExpressionFactory.Function(
                sqlFunctionName,
                newArguments,
                nullable: true,
                argumentsPropagateNullability: newArguments.Select(_ => true).ToArray(),
                method.ReturnType,
                typeMapping);
        }

        if (TruncateMethodInfos.Contains(method))
        {
            var argument = arguments[0];
            return _sqlExpressionFactory.Function(
                "TRUNC",
                [argument],
                nullable: true,
                argumentsPropagateNullability: [true],
                method.ReturnType,
                argument.TypeMapping);
        }

        // ROUND without precision
        if (RoundMethodInfos.Contains(method))
        {
            var argument = arguments[0];
            return _sqlExpressionFactory.Function(
                "ROUND",
                [argument],
                nullable: true,
                argumentsPropagateNullability: [true],
                method.ReturnType,
                argument.TypeMapping);
        }

        // ROUND with precision
        if (RoundWithPrecisionMethodInfos.Contains(method))
        {
            return _sqlExpressionFactory.Function(
                "ROUND",
                [arguments[0], arguments[1]],
                nullable: true,
                argumentsPropagateNullability: [true, true],
                method.ReturnType,
                arguments[0].TypeMapping);
        }

        // LOG with custom base: LOG(base, value)
        // Inverted compared to .NET Math.Log(value, base)
        if (LogWithBaseMethodInfos.Contains(method))
        {
            var value = arguments[0];
            var logBase = arguments[1];
            var typeMapping = ExpressionExtensions.InferTypeMapping(value, logBase);

            return _sqlExpressionFactory.Function(
                "LOG",
                [
                    _sqlExpressionFactory.ApplyTypeMapping(value, typeMapping),
                    _sqlExpressionFactory.ApplyTypeMapping(logBase, typeMapping)
                ],
                nullable: true,
                argumentsPropagateNullability: [true, true],
                method.ReturnType,
                typeMapping);
        }

        return null;
    }
}
