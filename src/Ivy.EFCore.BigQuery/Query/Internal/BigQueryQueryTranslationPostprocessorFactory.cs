using Microsoft.EntityFrameworkCore.Query;

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal;

/// <summary>
/// Factory for creating BigQuery-specific query translation postprocessors.
/// </summary>
public class BigQueryQueryTranslationPostprocessorFactory : IQueryTranslationPostprocessorFactory
{
    private readonly QueryTranslationPostprocessorDependencies _dependencies;
    private readonly RelationalQueryTranslationPostprocessorDependencies _relationalDependencies;

    public BigQueryQueryTranslationPostprocessorFactory(
        QueryTranslationPostprocessorDependencies dependencies,
        RelationalQueryTranslationPostprocessorDependencies relationalDependencies)
    {
        _dependencies = dependencies;
        _relationalDependencies = relationalDependencies;
    }

    public virtual QueryTranslationPostprocessor Create(QueryCompilationContext queryCompilationContext)
        => new BigQueryQueryTranslationPostprocessor(
            _dependencies,
            _relationalDependencies,
            (RelationalQueryCompilationContext)queryCompilationContext);
}
