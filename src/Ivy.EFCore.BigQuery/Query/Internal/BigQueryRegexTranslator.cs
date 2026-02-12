using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using System.Reflection;
using System.Text.RegularExpressions;
using ExpressionExtensions = Microsoft.EntityFrameworkCore.Query.ExpressionExtensions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal;

/// <summary>
///     Translates <see cref="Regex.IsMatch(string, string)"/> calls into BigQuery REGEXP_CONTAINS expressions.
/// </summary>
/// <remarks>
///     See: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#regexp_contains
/// </remarks>
public class BigQueryRegexTranslator : IMethodCallTranslator
{
    private static readonly MethodInfo IsMatch =
        typeof(Regex).GetRuntimeMethod(nameof(Regex.IsMatch), [typeof(string), typeof(string)])!;

    private static readonly MethodInfo IsMatchWithRegexOptions =
        typeof(Regex).GetRuntimeMethod(nameof(Regex.IsMatch), [typeof(string), typeof(string), typeof(RegexOptions)])!;

    private readonly ISqlExpressionFactory _sqlExpressionFactory;

    public BigQueryRegexTranslator(ISqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (method != IsMatch && method != IsMatchWithRegexOptions)
        {
            return null;
        }

        var (input, pattern) = (arguments[0], arguments[1]);
        var typeMapping = ExpressionExtensions.InferTypeMapping(input, pattern);

        // For IsMatchWithRegexOptions, we only support RegexOptions.None or RegexOptions.IgnoreCase
        if (method == IsMatchWithRegexOptions)
        {
            if (arguments[2] is not SqlConstantExpression { Value: RegexOptions options })
            {
                return null;
            }

            if (options != RegexOptions.None && options != RegexOptions.IgnoreCase)
            {
                return null;
            }

            if (options == RegexOptions.IgnoreCase && pattern is SqlConstantExpression { Value: string patternValue })
            {
                pattern = _sqlExpressionFactory.Constant("(?i)" + patternValue, typeMapping);
            }
            else if (options == RegexOptions.IgnoreCase)
            {
                return null;
            }
        }

        return _sqlExpressionFactory.Function(
            "REGEXP_CONTAINS",
            [
                _sqlExpressionFactory.ApplyTypeMapping(input, typeMapping),
                _sqlExpressionFactory.ApplyTypeMapping(pattern, typeMapping)
            ],
            nullable: true,
            argumentsPropagateNullability: [true, true],
            typeof(bool));
    }
}