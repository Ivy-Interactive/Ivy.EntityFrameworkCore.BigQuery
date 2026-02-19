using Microsoft.EntityFrameworkCore.Query;
using System.Linq.Expressions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal;

#pragma warning disable EF1001 // Internal EF Core API usage.

public class BigQueryParameterBasedSqlProcessor : RelationalParameterBasedSqlProcessor
{
    private readonly BigQueryCorrelatedJoinPostprocessor _correlatedJoinPostprocessor;

    public BigQueryParameterBasedSqlProcessor(
        RelationalParameterBasedSqlProcessorDependencies dependencies,
        RelationalParameterBasedSqlProcessorParameters parameters)
        : base(dependencies, parameters)
    {
        _correlatedJoinPostprocessor = new BigQueryCorrelatedJoinPostprocessor(
            dependencies.SqlExpressionFactory,
            dependencies.TypeMappingSource);
    }

    public override Expression Optimize(
        Expression queryExpression,
        IReadOnlyDictionary<string, object?> parametersValues,
        out bool canCache)
    {
        // Transform correlated JOINs - handles CrossJoinExpression with correlated inner SELECTs
        queryExpression = _correlatedJoinPostprocessor.Visit(queryExpression);

        return base.Optimize(queryExpression, parametersValues, out canCache);
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
