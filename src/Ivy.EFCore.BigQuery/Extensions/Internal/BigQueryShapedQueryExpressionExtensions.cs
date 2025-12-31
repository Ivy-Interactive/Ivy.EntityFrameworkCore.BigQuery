using Ivy.EntityFrameworkCore.BigQuery.Query.Internal;
using Ivy.EntityFrameworkCore.BigQuery.Storage.Internal.Mapping;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;

namespace Ivy.EntityFrameworkCore.BigQuery.Extensions.Internal;

/// <summary>
/// Extension methods for ShapedQueryExpression to help extract and optimize BigQuery array operations.
/// </summary>
internal static class BigQueryShapedQueryExpressionExtensions
{
    /// <summary>
    /// If the given <paramref name="source" /> wraps an array-returning expression without any additional clauses (e.g. filter,
    /// ordering...), returns that expression.
    /// </summary>
    public static bool TryExtractArray(
        this ShapedQueryExpression source,
        [NotNullWhen(true)] out SqlExpression? array,
        bool ignoreOrderings = false)
        => TryExtractArray(source, out array, out _, ignoreOrderings);

    /// <summary>
    /// If the given <paramref name="source" /> wraps an array-returning expression without any additional clauses (e.g. filter,
    /// ordering...), returns that expression.
    /// </summary>
    public static bool TryExtractArray(
        this ShapedQueryExpression source,
        [NotNullWhen(true)] out SqlExpression? array,
        [NotNullWhen(true)] out ColumnExpression? projectedColumn,
        bool ignoreOrderings = false)
    {
        if (source.QueryExpression is SelectExpression
            {
                Tables: [BigQueryUnnestExpression { Array: var a } unnest],
                GroupBy: [],
                Having: null,
                IsDistinct: false,
                Limit: null,
                Offset: null,
                Predicate: null
            } select2
            // We can only apply the indexing if the array is ordered by its natural order, (OFFSET) column that
            // we created in TranslatePrimitiveCollection. For example, if another ordering has been applied, we can no longer
            // simply index into the original array.
            && (ignoreOrderings
                || select2.Orderings.Count == 0
                || (select2.Orderings.Count == 1
                    && select2.Orderings[0].Expression is ColumnExpression { Name: "offset", TableAlias: var orderingTableAlias }
                    && orderingTableAlias == unnest.Alias))
            && a.TypeMapping is BigQueryArrayTypeMapping
            && TryGetProjectedColumn(source, out var column))
        {
            array = a;
            projectedColumn = column;
            return true;
        }

        array = null;
        projectedColumn = null;
        return false;
    }

    private static bool TryGetProjectedColumn(
        ShapedQueryExpression shapedQueryExpression,
        [NotNullWhen(true)] out ColumnExpression? projectedColumn)
    {
        var shaperExpression = shapedQueryExpression.ShaperExpression;

        // Unwrap Convert expressions for nullable types
        if (shaperExpression is UnaryExpression { NodeType: ExpressionType.Convert } unaryExpression
            && unaryExpression.Operand.Type.IsGenericType
            && unaryExpression.Operand.Type.GetGenericTypeDefinition() == typeof(Nullable<>)
            && Nullable.GetUnderlyingType(unaryExpression.Operand.Type) == unaryExpression.Type)
        {
            shaperExpression = unaryExpression.Operand;
        }

        if (shaperExpression is ProjectionBindingExpression projectionBindingExpression
            && shapedQueryExpression.QueryExpression is SelectExpression selectExpression
            && selectExpression.GetProjection(projectionBindingExpression) is ColumnExpression c)
        {
            projectedColumn = c;
            return true;
        }

        projectedColumn = null;
        return false;
    }
}
