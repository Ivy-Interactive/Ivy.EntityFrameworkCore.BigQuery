using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
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
        // Save UpdateExpression tags before processing - UpdateExpression.VisitChildren()
        // uses a constructor that drops tags when the expression is rebuilt by visitors.
        var updateTags = (queryExpression as UpdateExpression)?.Tags;

        // Transform correlated JOINs - handles CrossJoinExpression with correlated inner SELECTs
        queryExpression = _correlatedJoinPostprocessor.Visit(queryExpression);

        queryExpression = base.Process(queryExpression, parametersDecorator);

        queryExpression = new BigQueryDeleteConvertingExpressionVisitor().Process(queryExpression);

        // Restore tags if they were dropped during visitor processing
        if (updateTags is { Count: > 0 } && queryExpression is UpdateExpression update && update.Tags.Count == 0)
        {
            queryExpression = update.ApplyTags(updateTags);
        }

        return queryExpression;
    }

    /// <inheritdoc />
    protected override Expression ProcessSqlNullability(Expression queryExpression, ParametersCacheDecorator decorator)
        => new BigQuerySqlNullabilityProcessor(Dependencies, Parameters).Process(queryExpression, decorator);
}
