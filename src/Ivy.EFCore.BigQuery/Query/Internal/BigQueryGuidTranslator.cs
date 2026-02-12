using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using System.Reflection;

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal;

/// <summary>
///     Translates <see cref="Guid"/> methods into BigQuery SQL functions.
/// </summary>
/// <remarks>
///     See: https://cloud.google.com/bigquery/docs/reference/standard-sql/uuid_functions
/// </remarks>
public class BigQueryGuidTranslator : IMethodCallTranslator
{
    private static readonly MethodInfo NewGuid =
        typeof(Guid).GetRuntimeMethod(nameof(Guid.NewGuid), Type.EmptyTypes)!;

    private readonly ISqlExpressionFactory _sqlExpressionFactory;

    public BigQueryGuidTranslator(ISqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (method == NewGuid)
        {
            return _sqlExpressionFactory.Function(
                "GENERATE_UUID",
                [],
                nullable: false,
                argumentsPropagateNullability: [],
                typeof(Guid));
        }

        return null;
    }
}