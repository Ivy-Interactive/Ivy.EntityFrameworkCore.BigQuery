using Microsoft.EntityFrameworkCore.Query;
using System.Linq.Expressions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal;

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

    /// <inheritdoc />
    public override Expression Process(Expression queryExpression, ParametersCacheDecorator parametersDecorator)
    {
        // Transform correlated JOINs - handles CrossJoinExpression with correlated inner SELECTs
        queryExpression = _correlatedJoinPostprocessor.Visit(queryExpression);

        queryExpression = base.Process(queryExpression, parametersDecorator);

        queryExpression = new BigQueryDeleteConvertingExpressionVisitor().Process(queryExpression);

        return queryExpression;
    }

    /// <inheritdoc />
    protected override Expression ProcessSqlNullability(Expression queryExpression, ParametersCacheDecorator decorator)
        => new BigQuerySqlNullabilityProcessor(Dependencies, Parameters).Process(queryExpression, decorator);
}
