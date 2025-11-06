using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using System.Reflection;

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal;

/// <summary>
///     Provides translation services for string member access to BigQuery SQL functions.
/// </summary>
public class BigQueryStringMemberTranslator : IMemberTranslator
{
    private readonly ISqlExpressionFactory _sqlExpressionFactory;

    private static readonly MemberInfo StringLength
        = typeof(string).GetProperty(nameof(string.Length))!;

    public BigQueryStringMemberTranslator(ISqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MemberInfo member,
        Type returnType,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (member == StringLength && instance?.Type == typeof(string))
        {
            return _sqlExpressionFactory.Function(
                "LENGTH",
                [instance],
                nullable: true,
                argumentsPropagateNullability: [true],
                typeof(int));
        }

        return null;
    }
}
