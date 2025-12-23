using Microsoft.EntityFrameworkCore.Query;
using System.Linq.Expressions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal;

public class BigQueryParameterBasedSqlProcessor : RelationalParameterBasedSqlProcessor
{
    public BigQueryParameterBasedSqlProcessor(
        RelationalParameterBasedSqlProcessorDependencies dependencies,
        RelationalParameterBasedSqlProcessorParameters parameters)
        : base(dependencies, parameters)
    {
    }

    protected override Expression ProcessSqlNullability(
        Expression selectExpression,
        IReadOnlyDictionary<string, object?> parametersValues,
        out bool canCache)
    {
        return new BigQuerySqlNullabilityProcessor(Dependencies, Parameters)
            .Process(selectExpression, parametersValues, out canCache);
    }
}
