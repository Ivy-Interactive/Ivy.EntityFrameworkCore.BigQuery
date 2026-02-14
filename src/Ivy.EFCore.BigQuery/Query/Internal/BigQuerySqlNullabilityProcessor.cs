using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal;

public class BigQuerySqlNullabilityProcessor : SqlNullabilityProcessor
{
    public BigQuerySqlNullabilityProcessor(
        RelationalParameterBasedSqlProcessorDependencies dependencies,
        RelationalParameterBasedSqlProcessorParameters parameters)
        : base(dependencies, parameters)
    {
    }

    /// <summary>
    /// Visits a join predicate with more permissive handling than the base class.
    /// The base ProcessJoinPredicate only handles specific SqlBinaryExpression types,
    /// but BigQuery queries can have SqlUnaryExpression and other types in join predicates.
    /// </summary>
    private SqlExpression VisitJoinPredicate(SqlExpression predicate)
    {
        // For join predicates, we visit with optimized expansion allowed
        // This handles all expression types that the base ProcessJoinPredicate doesn't support
        return Visit(predicate, allowOptimizedExpansion: true, out _);
    }

    protected override TableExpressionBase Visit(TableExpressionBase tableExpressionBase)
    {
        switch (tableExpressionBase)
        {
            case BigQueryUnnestExpression unnestExpression:
            {
                // Visit the array expression inside UNNEST
                var visitedArray = Visit(unnestExpression.Array, allowOptimizedExpansion: true, out _);
                return unnestExpression.Update((SqlExpression)visitedArray);
            }

            case InnerJoinExpression innerJoinExpression:
            {
                var newTable = Visit(innerJoinExpression.Table);
                var newJoinPredicate = VisitJoinPredicate(innerJoinExpression.JoinPredicate);

                return innerJoinExpression.Update(newTable, newJoinPredicate);
            }

            case LeftJoinExpression leftJoinExpression:
            {
                var newTable = Visit(leftJoinExpression.Table);
                var newJoinPredicate = VisitJoinPredicate(leftJoinExpression.JoinPredicate);

                return leftJoinExpression.Update(newTable, newJoinPredicate);
            }

            default:
                return base.Visit(tableExpressionBase);
        }
    }

    protected override SqlExpression VisitCustomSqlExpression(
        SqlExpression sqlExpression,
        bool allowOptimizedExpansion,
        out bool nullable)
    {
        switch (sqlExpression)
        {
            case BigQueryArrayIndexExpression arrayIndexExpression:
            {
                // Visit the array and index expressions
                var visitedArray = Visit(arrayIndexExpression.Array, allowOptimizedExpansion, out var arrayNullable);
                var visitedIndex = Visit(arrayIndexExpression.Index, allowOptimizedExpansion, out _);

                // Array indexing can return null if the index is out of bounds or the array is null
                // In BigQuery, accessing arr[OFFSET(i)] returns NULL if index is out of range
                nullable = true;

                return arrayIndexExpression.Update((SqlExpression)visitedArray, (SqlExpression)visitedIndex);
            }

            case BigQueryStructAccessExpression structAccessExpression:
            {
                var visitedStruct = Visit(structAccessExpression.Struct, allowOptimizedExpansion, out var structNullable);

                nullable = structNullable;

                return structAccessExpression.Update((SqlExpression)visitedStruct);
            }

            case BigQueryStructConstructorExpression structConstructorExpression:
            {
                var visitedArguments = new List<SqlExpression>();
                var anyNullable = false;

                foreach (var argument in structConstructorExpression.Arguments)
                {
                    var visitedArg = Visit(argument, allowOptimizedExpansion, out var argNullable);
                    visitedArguments.Add((SqlExpression)visitedArg);
                    anyNullable |= argNullable;
                }

                nullable = anyNullable;

                return structConstructorExpression.Update(visitedArguments);
            }

            case BigQueryInUnnestExpression inUnnestExpression:
            {
                var visitedItem = Visit(inUnnestExpression.Item, allowOptimizedExpansion, out _);
                var visitedArray = Visit(inUnnestExpression.Array, allowOptimizedExpansion, out _);

                // IN UNNEST returns a boolean, never null
                nullable = false;

                return inUnnestExpression.Update((SqlExpression)visitedItem, (SqlExpression)visitedArray);
            }

            case BigQueryJsonTraversalExpression jsonTraversalExpression:
            {
                var visitedExpression = Visit(jsonTraversalExpression.Expression, allowOptimizedExpansion, out var expressionNullable);
                var visitedPath = new List<SqlExpression>();

                foreach (var pathComponent in jsonTraversalExpression.Path)
                {
                    var visitedComponent = Visit(pathComponent, allowOptimizedExpansion, out _);
                    visitedPath.Add((SqlExpression)visitedComponent);
                }

                // Traversal can return null if the path doesn't exist or base is null
                nullable = true;

                return jsonTraversalExpression.Update((SqlExpression)visitedExpression, visitedPath);
            }

            case BigQueryExtractExpression extractExpression:
            {
                var visitedArgument = Visit(extractExpression.Argument, allowOptimizedExpansion, out var argumentNullable);

                // EXTRACT returns null if the argument is null
                nullable = argumentNullable;

                return visitedArgument != extractExpression.Argument
                    ? new BigQueryExtractExpression(extractExpression.Part, (SqlExpression)visitedArgument, extractExpression.Type, extractExpression.TypeMapping)
                    : extractExpression;
            }

            case BigQueryIntervalExpression intervalExpression:
            {
                var visitedValue = Visit(intervalExpression.Value, allowOptimizedExpansion, out var valueNullable);

                // INTERVAL returns null if the value is null
                nullable = valueNullable;

                return visitedValue != intervalExpression.Value
                    ? new BigQueryIntervalExpression((SqlExpression)visitedValue, intervalExpression.DatePart, intervalExpression.TypeMapping)
                    : intervalExpression;
            }

            case BigQueryArrayLiteralExpression arrayLiteralExpression:
            {
                var visitedElements = new List<SqlExpression>();
                var changed = false;

                foreach (var element in arrayLiteralExpression.Elements)
                {
                    var visitedElement = Visit(element, allowOptimizedExpansion, out _);
                    visitedElements.Add((SqlExpression)visitedElement);
                    if (visitedElement != element)
                    {
                        changed = true;
                    }
                }

                nullable = false;

                return changed
                    ? arrayLiteralExpression.Update(visitedElements)
                    : arrayLiteralExpression;
            }

            default:
                return base.VisitCustomSqlExpression(sqlExpression, allowOptimizedExpansion, out nullable);
        }
    }
}
