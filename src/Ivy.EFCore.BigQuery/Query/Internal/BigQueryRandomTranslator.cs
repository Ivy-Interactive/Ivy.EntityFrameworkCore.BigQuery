using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal;

/// <summary>
/// Translates <see cref="DbFunctionsExtensions.Random(DbFunctions)"/> to BigQuery's RAND() function.
/// </summary>
public class BigQueryRandomTranslator : IMethodCallTranslator
{
    private static readonly MethodInfo _methodInfo
        = typeof(DbFunctionsExtensions).GetRuntimeMethod(nameof(DbFunctionsExtensions.Random), [typeof(DbFunctions)])!;

    private readonly ISqlExpressionFactory _sqlExpressionFactory;

    public BigQueryRandomTranslator(ISqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    public virtual SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        return _methodInfo.Equals(method)
            ? _sqlExpressionFactory.Function(
                "RAND",
                [],
                nullable: false,
                argumentsPropagateNullability: [],
                method.ReturnType)
            : null;
    }
}
