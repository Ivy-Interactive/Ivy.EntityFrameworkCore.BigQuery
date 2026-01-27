using Ivy.EntityFrameworkCore.BigQuery.Extensions.Internal;
using Ivy.EntityFrameworkCore.BigQuery.Storage.Internal.Mapping;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
using System.Linq.Expressions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal;
#pragma warning disable EF1001 // Internal EF Core API usage.

public class BigQueryQueryableMethodTranslatingExpressionVisitor : RelationalQueryableMethodTranslatingExpressionVisitor
{
    private readonly RelationalQueryCompilationContext _queryCompilationContext;
    private readonly IRelationalTypeMappingSource _typeMappingSource;
    private readonly BigQuerySqlExpressionFactory _sqlExpressionFactory;

    public BigQueryQueryableMethodTranslatingExpressionVisitor(
        QueryableMethodTranslatingExpressionVisitorDependencies dependencies,
        RelationalQueryableMethodTranslatingExpressionVisitorDependencies relationalDependencies,
        RelationalQueryCompilationContext queryCompilationContext)
        : base(dependencies, relationalDependencies, queryCompilationContext)
    {
        _queryCompilationContext = queryCompilationContext;
        _typeMappingSource = relationalDependencies.TypeMappingSource;
        _sqlExpressionFactory = (BigQuerySqlExpressionFactory)relationalDependencies.SqlExpressionFactory;
    }

    protected BigQueryQueryableMethodTranslatingExpressionVisitor(
        BigQueryQueryableMethodTranslatingExpressionVisitor parentVisitor)
        : base(parentVisitor)
    {
        _queryCompilationContext = parentVisitor._queryCompilationContext;
        _typeMappingSource = parentVisitor._typeMappingSource;
        _sqlExpressionFactory = parentVisitor._sqlExpressionFactory;
    }

        protected override QueryableMethodTranslatingExpressionVisitor CreateSubqueryVisitor()
            => new BigQueryQueryableMethodTranslatingExpressionVisitor(this);

    /// <inheritdoc/>
    protected override ShapedQueryExpression? TranslatePrimitiveCollection(
        SqlExpression sqlExpression,
        IProperty? property,
        string tableAlias)
    {
        if (sqlExpression.TypeMapping is not BigQueryArrayTypeMapping arrayTypeMapping)
        {
            return null;
        }

        var elementClrType = sqlExpression.Type.IsArray
            ? sqlExpression.Type.GetElementType()!
            : sqlExpression.Type.GetGenericArguments()[0];

        var elementTypeMapping = arrayTypeMapping.ElementTypeMapping;

        var isElementNullable = property?.GetElementType() is null
            ? (Nullable.GetUnderlyingType(elementClrType) != null || !elementClrType.IsValueType)
            : property.GetElementType()!.IsNullable;

        // UNNEST with WITH OFFSET for ordering
        var unnestExpression = new BigQueryUnnestExpression(
            sqlExpression,
            tableAlias,
            elementClrType,
            elementTypeMapping,
            withOffset: true,
            offsetAlias: "offset");

        // Create a column expression for the unnested value
        // In BigQuery UNNEST, when you don't specify a column name, the value is accessed
        // directly through the table alias without column qualification
        // We use "value" as the column name to avoid conflicts
        var valueColumn = new ColumnExpression(
            "value",
            tableAlias,
            Nullable.GetUnderlyingType(elementClrType) ?? elementClrType,  // Unwrap nullable
            elementTypeMapping,
            isElementNullable);

        var offsetTypeMapping = _typeMappingSource.FindMapping(typeof(long))!;

        // Create initial offset column for identifier (will be recreated with actual alias for ordering)
        var offsetColumn = new ColumnExpression(
            "offset",
            tableAlias,
            typeof(long),
            offsetTypeMapping,
            nullable: false);

        var selectExpression = new SelectExpression(
            new List<TableExpressionBase> { unnestExpression },
            valueColumn,
            identifier: new List<(ColumnExpression, ValueComparer)> { (offsetColumn, offsetTypeMapping.Comparer) },
            _queryCompilationContext.SqlAliasManager);

        // Get the actual alias from the SelectExpression - SqlAliasManager may have uniquified it
        // This ensures the ordering column references the correct table alias for TryExtractArray to work
        var actualAlias = selectExpression.Tables[0].Alias!;
        var orderingOffsetColumn = new ColumnExpression(
            "offset",
            actualAlias,
            typeof(long),
            offsetTypeMapping,
            nullable: false);

        // Explicit ordering by offset to preserve array order
        selectExpression.AppendOrdering(new OrderingExpression(orderingOffsetColumn, ascending: true));

        var shaperType = elementClrType.IsValueType && Nullable.GetUnderlyingType(elementClrType) == null
            ? typeof(Nullable<>).MakeGenericType(elementClrType)
            : elementClrType;

        Expression shaperExpression = new ProjectionBindingExpression(
            selectExpression,
            new ProjectionMember(),
            shaperType);
        if (elementClrType != shaperExpression.Type)
        {
            shaperExpression = Expression.Convert(shaperExpression, elementClrType);
        }

        return new ShapedQueryExpression(selectExpression, shaperExpression);
    }

    /// <summary>
    /// Translates Count() on array properties to ARRAY_LENGTH() function.
    /// This handles both e.Array.Length and e.Array.Count() since EF Core normalizes Length to Count().
    /// </summary>
    protected override ShapedQueryExpression? TranslateCount(ShapedQueryExpression source, LambdaExpression? predicate)
    {
        // Simplify e.Array.Count() => ARRAY_LENGTH(e.Array) instead of SELECT COUNT(*) FROM UNNEST(e.Array)
        if (predicate is null && source.TryExtractArray(out var array, ignoreOrderings: true))
        {
            var translation = _sqlExpressionFactory.Function(
                "ARRAY_LENGTH",
                new[] { array },
                nullable: true,
                argumentsPropagateNullability: new[] { true },
                typeof(int));

            var selectExpression = new SelectExpression(translation, _queryCompilationContext.SqlAliasManager);

            return source.Update(
                selectExpression,
                Expression.Convert(
                    new ProjectionBindingExpression(selectExpression, new ProjectionMember(), typeof(int?)),
                    typeof(int)));
        }

        return base.TranslateCount(source, predicate);
    }

    /// <summary>
    /// Translates ElementAt/ElementAtOrDefault on array properties to array indexing syntax array[OFFSET(index)].
    /// This handles arr[i] and arr.ElementAt(i) operations.
    /// </summary>
    protected override ShapedQueryExpression? TranslateElementAtOrDefault(
        ShapedQueryExpression source,
        Expression index,
        bool returnDefault)
    {
        // Simplify x.Array[i] => x.Array[OFFSET(i)] instead of a subquery with LIMIT/OFFSET
        if (!returnDefault
            && source.TryExtractArray(out var array, out var projectedColumn)
            && TranslateExpression(index) is { } translatedIndex)
        {
            var arrayIndexExpression = _sqlExpressionFactory.ArrayIndex(array, translatedIndex);
            var selectExpression = new SelectExpression(arrayIndexExpression, _queryCompilationContext.SqlAliasManager);

            return source.Update(
                selectExpression,
                source.ShaperExpression);
        }

        return base.TranslateElementAtOrDefault(source, index, returnDefault);
    }

}