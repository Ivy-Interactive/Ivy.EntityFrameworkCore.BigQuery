using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using System.Linq.Expressions;
using System.Reflection;
using ExpressionExtensions = Microsoft.EntityFrameworkCore.Query.ExpressionExtensions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal;

/// <summary>
///     Provides translation services for string instance methods to BigQuery SQL functions.
/// </summary>
/// <remarks>
///     See: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions
/// </remarks>
public class BigQueryStringMethodTranslator : IMethodCallTranslator
{
    private readonly ISqlExpressionFactory _sqlExpressionFactory;

    #region MethodInfo Definitions

    private static readonly MethodInfo IndexOfString
        = typeof(string).GetRuntimeMethod(nameof(string.IndexOf), [typeof(string)])!;

    private static readonly MethodInfo IndexOfStringWithStartIndex
        = typeof(string).GetRuntimeMethod(nameof(string.IndexOf), [typeof(string), typeof(int)])!;

    private static readonly MethodInfo IndexOfChar
        = typeof(string).GetRuntimeMethod(nameof(string.IndexOf), [typeof(char)])!;

    private static readonly MethodInfo Replace
        = typeof(string).GetRuntimeMethod(nameof(string.Replace), [typeof(string), typeof(string)])!;

    private static readonly MethodInfo ToLower
        = typeof(string).GetRuntimeMethod(nameof(string.ToLower), Type.EmptyTypes)!;

    private static readonly MethodInfo ToUpper
        = typeof(string).GetRuntimeMethod(nameof(string.ToUpper), Type.EmptyTypes)!;

    private static readonly MethodInfo Substring
        = typeof(string).GetRuntimeMethod(nameof(string.Substring), [typeof(int)])!;

    private static readonly MethodInfo SubstringWithLength
        = typeof(string).GetRuntimeMethod(nameof(string.Substring), [typeof(int), typeof(int)])!;

    private static readonly MethodInfo StartsWith
        = typeof(string).GetRuntimeMethod(nameof(string.StartsWith), [typeof(string)])!;

    private static readonly MethodInfo EndsWith
        = typeof(string).GetRuntimeMethod(nameof(string.EndsWith), [typeof(string)])!;

    private static readonly MethodInfo Contains
        = typeof(string).GetRuntimeMethod(nameof(string.Contains), [typeof(string)])!;

    private static readonly MethodInfo TrimBothWithNoParam
        = typeof(string).GetRuntimeMethod(nameof(string.Trim), Type.EmptyTypes)!;

    private static readonly MethodInfo TrimBothWithChars
        = typeof(string).GetRuntimeMethod(nameof(string.Trim), [typeof(char[])])!;

    private static readonly MethodInfo TrimBothWithSingleChar
        = typeof(string).GetRuntimeMethod(nameof(string.Trim), [typeof(char)])!;

    private static readonly MethodInfo TrimStartWithNoParam
        = typeof(string).GetRuntimeMethod(nameof(string.TrimStart), Type.EmptyTypes)!;

    private static readonly MethodInfo TrimStartWithChars
        = typeof(string).GetRuntimeMethod(nameof(string.TrimStart), [typeof(char[])])!;

    private static readonly MethodInfo TrimStartWithSingleChar
        = typeof(string).GetRuntimeMethod(nameof(string.TrimStart), [typeof(char)])!;

    private static readonly MethodInfo TrimEndWithNoParam
        = typeof(string).GetRuntimeMethod(nameof(string.TrimEnd), Type.EmptyTypes)!;

    private static readonly MethodInfo TrimEndWithChars
        = typeof(string).GetRuntimeMethod(nameof(string.TrimEnd), [typeof(char[])])!;

    private static readonly MethodInfo TrimEndWithSingleChar
        = typeof(string).GetRuntimeMethod(nameof(string.TrimEnd), [typeof(char)])!;

    private static readonly MethodInfo IsNullOrEmpty
        = typeof(string).GetRuntimeMethod(nameof(string.IsNullOrEmpty), [typeof(string)])!;

    private static readonly MethodInfo IsNullOrWhiteSpace
        = typeof(string).GetRuntimeMethod(nameof(string.IsNullOrWhiteSpace), [typeof(string)])!;

    private static readonly MethodInfo FirstOrDefaultWithoutArgs
        = typeof(Enumerable).GetRuntimeMethods()
            .Single(m => m.Name == nameof(Enumerable.FirstOrDefault) && m.GetParameters().Length == 1)
            .MakeGenericMethod(typeof(char));

    private static readonly MethodInfo LastOrDefaultWithoutArgs
        = typeof(Enumerable).GetRuntimeMethods()
            .Single(m => m.Name == nameof(Enumerable.LastOrDefault) && m.GetParameters().Length == 1)
            .MakeGenericMethod(typeof(char));

    private static readonly MethodInfo PadLeft
        = typeof(string).GetRuntimeMethod(nameof(string.PadLeft), [typeof(int)])!;

    private static readonly MethodInfo PadLeftWithChar
        = typeof(string).GetRuntimeMethod(nameof(string.PadLeft), [typeof(int), typeof(char)])!;

    private static readonly MethodInfo PadRight
        = typeof(string).GetRuntimeMethod(nameof(string.PadRight), [typeof(int)])!;

    private static readonly MethodInfo PadRightWithChar
        = typeof(string).GetRuntimeMethod(nameof(string.PadRight), [typeof(int), typeof(char)])!;

    // String.Join method overloads
    private static readonly MethodInfo StringJoinWithObjectArray
        = typeof(string).GetRuntimeMethod(nameof(string.Join), [typeof(string), typeof(object[])])!;

    private static readonly MethodInfo StringJoinWithStringArray
        = typeof(string).GetRuntimeMethod(nameof(string.Join), [typeof(string), typeof(string[])])!;

    private static readonly MethodInfo StringJoinWithEnumerable
        = typeof(string).GetRuntimeMethod(nameof(string.Join), [typeof(string), typeof(IEnumerable<string>)])!;

    #endregion

    public BigQueryStringMethodTranslator(ISqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        // Handle instance methods
        if (instance != null)
        {
            return TranslateInstanceMethod(instance, method, arguments);
        }

        // Handle static methods
        return TranslateStaticMethod(method, arguments);
    }

    private SqlExpression? TranslateInstanceMethod(
        SqlExpression instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments)
    {
        // ToLower / ToUpper
        if (method == ToLower || method == ToUpper)
        {
            return _sqlExpressionFactory.Function(
                method == ToLower ? "LOWER" : "UPPER",
                [instance],
                nullable: true,
                argumentsPropagateNullability: [true],
                typeof(string),
                instance.TypeMapping);
        }

        // Replace
        if (method == Replace)
        {
            var oldValue = arguments[0];
            var newValue = arguments[1];
            var stringTypeMapping = ExpressionExtensions.InferTypeMapping(instance, oldValue, newValue);

            return _sqlExpressionFactory.Function(
                "REPLACE",
                [
                    _sqlExpressionFactory.ApplyTypeMapping(instance, stringTypeMapping),
                    _sqlExpressionFactory.ApplyTypeMapping(oldValue, stringTypeMapping),
                    _sqlExpressionFactory.ApplyTypeMapping(newValue, stringTypeMapping)
                ],
                nullable: true,
                argumentsPropagateNullability: [true, true, true],
                typeof(string),
                stringTypeMapping);
        }

        // Substring
        if (method == Substring || method == SubstringWithLength)
        {
            // BigQuery SUBSTR is 1-indexed, .NET is 0-indexed
            // SUBSTR(str, pos, [length])
            var startIndex = arguments[0];
            var oneBasedIndex = startIndex is SqlConstantExpression { Value: int constantValue }
                ? _sqlExpressionFactory.Constant(constantValue + 1)
                : _sqlExpressionFactory.Add(startIndex, _sqlExpressionFactory.Constant(1));

            var substringArgs = method == Substring
                ? new[] { instance, oneBasedIndex }
                : new[] { instance, oneBasedIndex, arguments[1] };

            return _sqlExpressionFactory.Function(
                "SUBSTR",
                substringArgs,
                nullable: true,
                argumentsPropagateNullability: substringArgs.Select(_ => true).ToArray(),
                typeof(string),
                instance.TypeMapping);
        }

        // IndexOf
        if (method == IndexOfString || method == IndexOfChar)
        {
            return TranslateIndexOf(instance, arguments[0], startIndex: null);
        }

        if (method == IndexOfStringWithStartIndex)
        {
            return TranslateIndexOf(instance, arguments[0], arguments[1]);
        }

        // StartsWith
        if (method == StartsWith)
        {
            var pattern = arguments[0];
            var stringTypeMapping = ExpressionExtensions.InferTypeMapping(instance, pattern);

            instance = _sqlExpressionFactory.ApplyTypeMapping(instance, stringTypeMapping);
            pattern = _sqlExpressionFactory.ApplyTypeMapping(pattern, stringTypeMapping);

            // For constants, use LIKE with pattern
            if (pattern is SqlConstantExpression { Value: string patternValue })
            {
                return patternValue switch
                {
                    null => _sqlExpressionFactory.Like(instance, _sqlExpressionFactory.Constant(null, typeof(string), stringTypeMapping)),
                    "" => _sqlExpressionFactory.Like(instance, _sqlExpressionFactory.Constant("%")),
                    _ => _sqlExpressionFactory.Like(instance, _sqlExpressionFactory.Constant(patternValue + "%"))
                };
            }

            // For non-constants, use STARTS_WITH function
            return _sqlExpressionFactory.Function(
                "STARTS_WITH",
                [instance, pattern],
                nullable: true,
                argumentsPropagateNullability: [true, true],
                typeof(bool));
        }

        // EndsWith
        if (method == EndsWith)
        {
            var pattern = arguments[0];
            var stringTypeMapping = ExpressionExtensions.InferTypeMapping(instance, pattern);

            instance = _sqlExpressionFactory.ApplyTypeMapping(instance, stringTypeMapping);
            pattern = _sqlExpressionFactory.ApplyTypeMapping(pattern, stringTypeMapping);

            // For constants, use LIKE with pattern
            if (pattern is SqlConstantExpression { Value: string patternValue })
            {
                return patternValue switch
                {
                    null => _sqlExpressionFactory.Like(instance, _sqlExpressionFactory.Constant(null, typeof(string), stringTypeMapping)),
                    "" => _sqlExpressionFactory.Like(instance, _sqlExpressionFactory.Constant("%")),
                    _ => _sqlExpressionFactory.Like(instance, _sqlExpressionFactory.Constant("%" + patternValue))
                };
            }

            // For non-constants, use ENDS_WITH function
            return _sqlExpressionFactory.Function(
                "ENDS_WITH",
                [instance, pattern],
                nullable: true,
                argumentsPropagateNullability: [true, true],
                typeof(bool));
        }

        // Contains
        if (method == Contains)
        {
            var pattern = arguments[0];
            var stringTypeMapping = ExpressionExtensions.InferTypeMapping(instance, pattern);

            instance = _sqlExpressionFactory.ApplyTypeMapping(instance, stringTypeMapping);
            pattern = _sqlExpressionFactory.ApplyTypeMapping(pattern, stringTypeMapping);

            // For constants, use LIKE with pattern
            if (pattern is SqlConstantExpression { Value: string patternValue })
            {
                return patternValue switch
                {
                    null => _sqlExpressionFactory.Like(instance, _sqlExpressionFactory.Constant(null, typeof(string), stringTypeMapping)),
                    "" => _sqlExpressionFactory.Like(instance, _sqlExpressionFactory.Constant("%")),
                    _ => _sqlExpressionFactory.Like(instance, _sqlExpressionFactory.Constant("%" + patternValue + "%"))
                };
            }

            // For non-constants, use STRPOS > 0
            return _sqlExpressionFactory.GreaterThan(
                _sqlExpressionFactory.Function(
                    "STRPOS",
                    [instance, pattern],
                    nullable: true,
                    argumentsPropagateNullability: [true, true],
                    typeof(int)),
                _sqlExpressionFactory.Constant(0));
        }

        // Trim methods
        if (method == TrimBothWithNoParam || method == TrimBothWithChars || method == TrimBothWithSingleChar
            || method == TrimStartWithNoParam || method == TrimStartWithChars || method == TrimStartWithSingleChar
            || method == TrimEndWithNoParam || method == TrimEndWithChars || method == TrimEndWithSingleChar)
        {
            return TranslateTrim(instance, method, arguments);
        }

        // PadLeft / PadRight
        if (method == PadLeft || method == PadLeftWithChar || method == PadRight || method == PadRightWithChar)
        {
            var functionName = method == PadLeft || method == PadLeftWithChar ? "LPAD" : "RPAD";
            var length = arguments[0];

            // Check if we have a padding character
            SqlExpression? padChar = null;
            if (method == PadLeftWithChar || method == PadRightWithChar)
            {
                if (arguments[1] is SqlConstantExpression { Value: char c })
                {
                    padChar = _sqlExpressionFactory.Constant(c.ToString(), instance.TypeMapping);
                }
                else
                {
                    // If the pad character is not a constant, we need to convert it to string
                    // BigQuery LPAD/RPAD expect a string, not a char
                    padChar = _sqlExpressionFactory.Function(
                        "CAST",
                        [arguments[1]],
                        nullable: true,
                        argumentsPropagateNullability: [true],
                        typeof(string),
                        instance.TypeMapping);
                }
            }

            var padArguments = padChar != null
                ? new[] { instance, length, padChar }
                : new[] { instance, length };

            return _sqlExpressionFactory.Function(
                functionName,
                padArguments,
                nullable: true,
                argumentsPropagateNullability: padArguments.Select(_ => true).ToArray(),
                typeof(string),
                instance.TypeMapping);
        }

        return null;
    }

    private SqlExpression? TranslateStaticMethod(MethodInfo method, IReadOnlyList<SqlExpression> arguments)
    {
        // IsNullOrEmpty
        if (method == IsNullOrEmpty)
        {
            var argument = arguments[0];
            return _sqlExpressionFactory.OrElse(
                _sqlExpressionFactory.IsNull(argument),
                _sqlExpressionFactory.Equal(
                    argument,
                    _sqlExpressionFactory.Constant(string.Empty, argument.TypeMapping)));
        }

        // IsNullOrWhiteSpace
        if (method == IsNullOrWhiteSpace)
        {
            var argument = arguments[0];
            return _sqlExpressionFactory.OrElse(
                _sqlExpressionFactory.IsNull(argument),
                _sqlExpressionFactory.Equal(
                    _sqlExpressionFactory.Function(
                        "TRIM",
                        [argument],
                        nullable: true,
                        argumentsPropagateNullability: [true],
                        typeof(string),
                        argument.TypeMapping),
                    _sqlExpressionFactory.Constant(string.Empty, argument.TypeMapping)));
        }

        // FirstOrDefault (LINQ)
        if (method == FirstOrDefaultWithoutArgs)
        {
            var argument = arguments[0];
            return _sqlExpressionFactory.Function(
                "SUBSTR",
                [argument, _sqlExpressionFactory.Constant(1), _sqlExpressionFactory.Constant(1)],
                nullable: true,
                argumentsPropagateNullability: [true, true, true],
                typeof(string));
        }

        // LastOrDefault (LINQ)
        if (method == LastOrDefaultWithoutArgs)
        {
            var argument = arguments[0];
            return _sqlExpressionFactory.Function(
                "SUBSTR",
                [
                    argument,
                    _sqlExpressionFactory.Function(
                        "LENGTH",
                        [argument],
                        nullable: true,
                        argumentsPropagateNullability: [true],
                        typeof(int)),
                    _sqlExpressionFactory.Constant(1)
                ],
                nullable: true,
                argumentsPropagateNullability: [true, true, true],
                typeof(string));
        }

        // String.Join
        // Note: String.Join translation is complex because by the time expressions reach this translator,
        // LINQ expressions have already been converted to SQL expressions. To properly support String.Join,
        // we would need custom SQL expression types (like Npgsql's PgNewArrayExpression) or handle it
        // at an earlier stage in the query pipeline. For now, we return null to let EF Core handle it
        // client-side or through other means.
        if (method == StringJoinWithObjectArray || method == StringJoinWithStringArray || method == StringJoinWithEnumerable)
        {
            // Cannot translate String.Join for now - would require custom expression types
            // or preprocessing at an earlier stage
            return null;
        }

        return null;
    }

    private SqlExpression TranslateIndexOf(SqlExpression instance, SqlExpression searchExpression, SqlExpression? startIndex)
    {
        var stringTypeMapping = ExpressionExtensions.InferTypeMapping(instance, searchExpression);
        instance = _sqlExpressionFactory.ApplyTypeMapping(instance, stringTypeMapping);
        searchExpression = _sqlExpressionFactory.ApplyTypeMapping(searchExpression, stringTypeMapping);

        SqlExpression strposExpression;

        if (startIndex != null)
        {
            // For IndexOf with start index, we need to:
            // 1. Extract substring from startIndex onwards (convert to 1-based)
            // 2. Find position in that substring
            // 3. Add startIndex back to get final position
            // 4. Subtract 1 to convert from 1-based to 0-based

            var oneBasedStartIndex = startIndex is SqlConstantExpression { Value: int constantValue }
                ? _sqlExpressionFactory.Constant(constantValue + 1)
                : _sqlExpressionFactory.Add(startIndex, _sqlExpressionFactory.Constant(1));

            var substring = _sqlExpressionFactory.Function(
                "SUBSTR",
                [instance, oneBasedStartIndex],
                nullable: true,
                argumentsPropagateNullability: [true, true],
                typeof(string),
                stringTypeMapping);

            var position = _sqlExpressionFactory.Function(
                "STRPOS",
                [substring, searchExpression],
                nullable: true,
                argumentsPropagateNullability: [true, true],
                typeof(int));

            // If STRPOS returns 0 (not found), we want -1
            // If STRPOS returns > 0, we want position - 1 + startIndex
            strposExpression = _sqlExpressionFactory.Case(
                [
                    new CaseWhenClause(
                        _sqlExpressionFactory.Equal(position, _sqlExpressionFactory.Constant(0)),
                        _sqlExpressionFactory.Constant(-1))
                ],
                _sqlExpressionFactory.Add(
                    _sqlExpressionFactory.Subtract(position, _sqlExpressionFactory.Constant(1)),
                    startIndex));
        }
        else
        {
            // Simple case: STRPOS returns 1-based index, subtract 1 for 0-based
            // If not found, STRPOS returns 0, we want -1
            var position = _sqlExpressionFactory.Function(
                "STRPOS",
                [instance, searchExpression],
                nullable: true,
                argumentsPropagateNullability: [true, true],
                typeof(int));

            strposExpression = _sqlExpressionFactory.Subtract(position, _sqlExpressionFactory.Constant(1));
        }

        return strposExpression;
    }

    private SqlExpression? TranslateTrim(SqlExpression instance, MethodInfo method, IReadOnlyList<SqlExpression> arguments)
    {
        var isTrimStart = method == TrimStartWithNoParam || method == TrimStartWithChars || method == TrimStartWithSingleChar;
        var isTrimEnd = method == TrimEndWithNoParam || method == TrimEndWithChars || method == TrimEndWithSingleChar;
        var isTrimBoth = method == TrimBothWithNoParam || method == TrimBothWithChars || method == TrimBothWithSingleChar;

        var functionName = isTrimStart ? "LTRIM" : isTrimEnd ? "RTRIM" : "TRIM";

        // Check if we have characters to trim
        SqlExpression? trimCharsExpression = null;
        if (arguments.Count > 0 && arguments[0] is SqlConstantExpression constantTrimChars)
        {
            if (constantTrimChars.Value is char singleChar)
            {
                trimCharsExpression = _sqlExpressionFactory.Constant(singleChar.ToString(), instance.TypeMapping);
            }
            else if (constantTrimChars.Value is char[] charArray && charArray.Length > 0)
            {
                trimCharsExpression = _sqlExpressionFactory.Constant(new string(charArray), instance.TypeMapping);
            }
            // If empty array, treat as default whitespace trim
        }

        // BigQuery TRIM syntax: TRIM(value [, trim_characters])
        var trimArguments = trimCharsExpression != null
            ? new[] { instance, trimCharsExpression }
            : new[] { instance };

        return _sqlExpressionFactory.Function(
            functionName,
            trimArguments,
            nullable: true,
            argumentsPropagateNullability: trimArguments.Select(_ => true).ToArray(),
            typeof(string),
            instance.TypeMapping);
    }
}
