using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using System.Reflection;

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal;

/// <summary>
///     Translates methods defined on <see cref="Convert" /> into BigQuery CAST expressions.
/// </summary>
/// <remarks>
///     See: https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_functions
/// </remarks>
public class BigQueryConvertTranslator : IMethodCallTranslator
{
    private static readonly Dictionary<string, string> TypeMapping = new()
    {
        [nameof(Convert.ToBoolean)] = "BOOL",
        [nameof(Convert.ToByte)] = "INT64",
        [nameof(Convert.ToDecimal)] = "BIGNUMERIC",
        [nameof(Convert.ToDouble)] = "FLOAT64",
        [nameof(Convert.ToInt16)] = "INT64",
        [nameof(Convert.ToInt32)] = "INT64",
        [nameof(Convert.ToInt64)] = "INT64",
        [nameof(Convert.ToString)] = "STRING"
    };

    private static readonly List<Type> SupportedTypes =
    [
        typeof(bool),
        typeof(byte),
        typeof(decimal),
        typeof(double),
        typeof(float),
        typeof(int),
        typeof(long),
        typeof(short),
        typeof(string),
        typeof(object)
    ];

    private static readonly List<MethodInfo> SupportedMethods
        = TypeMapping.Keys
            .SelectMany(
                t => typeof(Convert).GetTypeInfo().GetDeclaredMethods(t)
                    .Where(
                        m => m.GetParameters().Length == 1
                            && SupportedTypes.Contains(m.GetParameters().First().ParameterType)))
            .ToList();

    private readonly ISqlExpressionFactory _sqlExpressionFactory;

    public BigQueryConvertTranslator(ISqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
        => SupportedMethods.Contains(method)
            ? _sqlExpressionFactory.Convert(arguments[0], method.ReturnType)
            : null;
}