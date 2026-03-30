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
        if (member.Name == nameof(string.Length) && member.DeclaringType == typeof(string) && instance is not null)
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
