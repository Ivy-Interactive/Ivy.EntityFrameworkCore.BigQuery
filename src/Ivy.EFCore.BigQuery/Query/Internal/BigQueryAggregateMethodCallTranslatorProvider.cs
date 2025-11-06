using Microsoft.EntityFrameworkCore.Query;

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal;

/// <summary>
/// Provider for BigQuery-specific aggregate method translators.
/// Registers translators that handle LINQ aggregate methods (Count, Sum, Average, Min, Max).
/// </summary>
public class BigQueryAggregateMethodCallTranslatorProvider : RelationalAggregateMethodCallTranslatorProvider
{
    public BigQueryAggregateMethodCallTranslatorProvider(
        RelationalAggregateMethodCallTranslatorProviderDependencies dependencies)
        : base(dependencies)
    {
        var sqlExpressionFactory = (BigQuerySqlExpressionFactory)dependencies.SqlExpressionFactory;
        var typeMappingSource = dependencies.RelationalTypeMappingSource;

        AddTranslators(
        [
            new BigQueryQueryableAggregateMethodTranslator(sqlExpressionFactory, typeMappingSource)
        ]);
    }
}
