using Microsoft.EntityFrameworkCore.Query;
using System.Linq.Expressions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal;

/// <summary>
/// BigQuery-specific query translation postprocessor.
/// Applies transformations to handle BigQuery limitations like correlated scalar subqueries
/// and APPLY expressions (which BigQuery doesn't support).
/// </summary>
public class BigQueryQueryTranslationPostprocessor : RelationalQueryTranslationPostprocessor
{
    private readonly BigQueryCorrelatedSubqueryPostprocessor _correlatedSubqueryPostprocessor;
    private readonly BigQueryApplyPostprocessor _applyPostprocessor;

    public BigQueryQueryTranslationPostprocessor(
        QueryTranslationPostprocessorDependencies dependencies,
        RelationalQueryTranslationPostprocessorDependencies relationalDependencies,
        RelationalQueryCompilationContext queryCompilationContext)
        : base(dependencies, relationalDependencies, queryCompilationContext)
    {
        _correlatedSubqueryPostprocessor = new BigQueryCorrelatedSubqueryPostprocessor(
            queryCompilationContext.SqlAliasManager,
            relationalDependencies.TypeMappingSource,
            relationalDependencies.SqlExpressionFactory);

        _applyPostprocessor = new BigQueryApplyPostprocessor(
            relationalDependencies.SqlExpressionFactory,
            relationalDependencies.TypeMappingSource);
    }

    public override Expression Process(Expression query)
    {
        var result = base.Process(query);

        // Transform OUTER APPLY / CROSS APPLY to LEFT JOIN / INNER JOIN
        // This must happen before correlated subquery processing
        result = _applyPostprocessor.Visit(result);

        // Transform correlated scalar subqueries to LEFT JOINs
        result = _correlatedSubqueryPostprocessor.Visit(result);

        return result;
    }
}