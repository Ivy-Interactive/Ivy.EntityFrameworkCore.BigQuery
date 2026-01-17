using Microsoft.EntityFrameworkCore.Query;
using System.Linq.Expressions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal;

/// <summary>
/// BigQuery-specific query translation postprocessor.
/// Applies transformations to handle BigQuery limitations like correlated scalar subqueries.
/// </summary>
public class BigQueryQueryTranslationPostprocessor : RelationalQueryTranslationPostprocessor
{
    private readonly BigQueryCorrelatedSubqueryPostprocessor _correlatedSubqueryPostprocessor;

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
    }

    public override Expression Process(Expression query)
    {
        var result = base.Process(query);

        // Transform correlated scalar subqueries to LEFT JOINs
        result = _correlatedSubqueryPostprocessor.Visit(result);

        return result;
    }
}