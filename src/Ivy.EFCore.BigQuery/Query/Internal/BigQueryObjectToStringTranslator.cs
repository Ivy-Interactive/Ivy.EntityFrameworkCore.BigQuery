using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using System.Reflection;

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal;

/// <summary>
///     Translates ToString() method calls on various types to BigQuery CAST expressions.
/// </summary>
public class BigQueryObjectToStringTranslator : IMethodCallTranslator
{
    private static readonly HashSet<Type> SupportedTypes =
    [
        typeof(sbyte),
        typeof(byte),
        typeof(short),
        typeof(ushort),
        typeof(int),
        typeof(uint),
        typeof(long),
        typeof(ulong),
        typeof(float),
        typeof(double),
        typeof(decimal),
        typeof(char),
        typeof(DateTime),
        typeof(DateOnly),
        typeof(TimeOnly),
        typeof(DateTimeOffset),
        typeof(TimeSpan),
        typeof(Guid)
    ];

    private readonly ISqlExpressionFactory _sqlExpressionFactory;

    public BigQueryObjectToStringTranslator(ISqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (instance == null || method.Name != nameof(ToString) || arguments.Count != 0)
        {
            return null;
        }

        if (instance.TypeMapping?.ClrType == typeof(string))
        {
            return instance;
        }

        // CAST(bool AS STRING) -> 'true'/'false'
        // .NET ToString() -> 'True'/'False'
        var underlyingType = Nullable.GetUnderlyingType(instance.Type) ?? instance.Type;
        if (underlyingType == typeof(bool))
        {

            if (instance.Type == typeof(bool?) || instance is not ColumnExpression { IsNullable: false })
            {
                return _sqlExpressionFactory.Case(
                    instance,
                    [
                        new CaseWhenClause(
                            _sqlExpressionFactory.Constant(false),
                            _sqlExpressionFactory.Constant("False")),
                        new CaseWhenClause(
                            _sqlExpressionFactory.Constant(true),
                            _sqlExpressionFactory.Constant("True"))
                    ],
                    _sqlExpressionFactory.Constant(string.Empty));
            }

            // Non-nullable booleans
            return _sqlExpressionFactory.Case(
                [
                    new CaseWhenClause(
                        instance,
                        _sqlExpressionFactory.Constant("True"))
                ],
                _sqlExpressionFactory.Constant("False"));
        }

        if (!SupportedTypes.Contains(underlyingType))
        {
            return null;
        }

        // COALESCE(CAST(value AS STRING), '')
        return _sqlExpressionFactory.Coalesce(
            _sqlExpressionFactory.Convert(instance, typeof(string)),
            _sqlExpressionFactory.Constant(string.Empty));
    }
}