using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
using System.Linq.Expressions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal;

#pragma warning disable EF1001 // Internal EF Core API usage.

/// <summary>
/// BigQuery doesn't support correlated scalar subqueries in SELECT projections.
/// This postprocessor transforms them into LEFT JOINs with ROW_NUMBER() window functions.
///
/// For example:
/// SELECT (SELECT o.OrderDate FROM Orders o WHERE c.CustomerID = o.CustomerID ORDER BY o.OrderID LIMIT 1)
/// FROM Customers c
///
/// Becomes:
/// SELECT subq.OrderDate
/// FROM Customers c
/// LEFT JOIN (
///     SELECT CustomerID, OrderDate, ROW_NUMBER() OVER (PARTITION BY CustomerID ORDER BY OrderID) AS rn
///     FROM Orders
///     WHERE OrderID &lt; 10500
/// ) subq ON c.CustomerID = subq.CustomerID AND subq.rn = 1
/// </summary>
public class BigQueryCorrelatedSubqueryPostprocessor : ExpressionVisitor
{
    private readonly SqlAliasManager _sqlAliasManager;
    private readonly IRelationalTypeMappingSource _typeMappingSource;
    private readonly ISqlExpressionFactory _sqlExpressionFactory;

    private SelectExpression? _currentSelect;
    private List<LeftJoinExpression>? _joinsToAdd;
    private HashSet<string>? _outerTableAliases;

    public BigQueryCorrelatedSubqueryPostprocessor(
        SqlAliasManager sqlAliasManager,
        IRelationalTypeMappingSource typeMappingSource,
        ISqlExpressionFactory sqlExpressionFactory)
    {
        _sqlAliasManager = sqlAliasManager;
        _typeMappingSource = typeMappingSource;
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    protected override Expression VisitExtension(Expression node)
    {
        switch (node)
        {
            case SelectExpression select:
                return VisitSelect(select);

            case ScalarSubqueryExpression scalarSubquery when _currentSelect is not null:
                return VisitScalarSubquery(scalarSubquery);

            case ShapedQueryExpression shapedQueryExpression:
                return shapedQueryExpression
                    .UpdateQueryExpression(Visit(shapedQueryExpression.QueryExpression))
                    .UpdateShaperExpression(Visit(shapedQueryExpression.ShaperExpression));

            default:
                return base.VisitExtension(node);
        }
    }

    private Expression VisitSelect(SelectExpression select)
    {
        var (parentSelect, parentJoinsToAdd, parentOuterAliases) =
            (_currentSelect, _joinsToAdd, _outerTableAliases);

        _currentSelect = select;
        _joinsToAdd = null;
        _outerTableAliases = CollectTableAliases(select);

        // Visit all parts of the select - scalar subqueries will be replaced inline
        var visitedSelect = (SelectExpression)base.VisitExtension(select);

        // If we found correlated scalar subqueries, add the JOINs
        if (_joinsToAdd is not null && _joinsToAdd.Count > 0)
        {
            var newTables = visitedSelect.Tables.ToList();
            newTables.AddRange(_joinsToAdd);

            visitedSelect = visitedSelect.Update(
                newTables,
                visitedSelect.Predicate,
                visitedSelect.GroupBy,
                visitedSelect.Having,
                visitedSelect.Projection,
                visitedSelect.Orderings,
                visitedSelect.Offset,
                visitedSelect.Limit);
        }

        (_currentSelect, _joinsToAdd, _outerTableAliases) =
            (parentSelect, parentJoinsToAdd, parentOuterAliases);

        return visitedSelect;
    }

    private Expression VisitScalarSubquery(ScalarSubqueryExpression scalarSubquery)
    {
        var subquery = scalarSubquery.Subquery;

        // Check if this is a correlated subquery
        var correlationInfo = AnalyzeCorrelation(subquery);
        if (!correlationInfo.IsCorrelated)
        {
            // Not correlated - let it pass through
            return base.VisitExtension(scalarSubquery);
        }

        // Transform the correlated scalar subquery to a LEFT JOIN
        // Returns the replacement column expression, or null if transformation failed
        var replacementColumn = TransformToLeftJoin(scalarSubquery, correlationInfo);
        if (replacementColumn is null)
        {
            // Could not transform - let it pass through (will fail at runtime)
            return base.VisitExtension(scalarSubquery);
        }

        // Return the replacement column - this replaces the scalar subquery in the expression tree
        return replacementColumn;
    }

    private CorrelationInfo AnalyzeCorrelation(SelectExpression subquery)
    {
        var finder = new OuterColumnFinder(_outerTableAliases ?? new HashSet<string>());
        finder.Visit(subquery);

        return new CorrelationInfo
        {
            IsCorrelated = finder.OuterColumns.Count > 0,
            OuterColumns = finder.OuterColumns,
            CorrelationPredicates = finder.CorrelationPredicates
        };
    }

    /// <summary>
    /// Transforms a correlated scalar subquery into a LEFT JOIN and returns the expression
    /// that should replace the scalar subquery. The LEFT JOIN is added to _joinsToAdd as a side effect.
    /// Returns a ColumnExpression for non-aggregates, or a COALESCE expression for aggregates.
    /// </summary>
    private SqlExpression? TransformToLeftJoin(
        ScalarSubqueryExpression scalarSubquery,
        CorrelationInfo correlationInfo)
    {
        var subquery = scalarSubquery.Subquery;

        if (subquery.Projection.Count != 1)
        {
            return null;
        }

        var projectedExpression = subquery.Projection[0].Expression;
        var projectedAlias = subquery.Projection[0].Alias;

        // Check if the projected expression contains nested correlated subqueries that reference outer tables.
        // If so, we cannot transform this subquery because those references would become invalid after
        // moving the subquery into a LEFT JOIN.
        if (ContainsNestedCorrelatedSubquery(projectedExpression, correlationInfo.OuterColumns))
        {
            return null;
        }

        // We need to figure out the partition columns (correlation columns from the inner query)
        // and build the ROW_NUMBER() expression
        var partitionColumns = new List<SqlExpression>();
        var joinPredicateParts = new List<SqlExpression>();
        SqlExpression? remainingPredicate = null;

        // Analyze the predicate to extract correlation conditions
        if (subquery.Predicate != null)
        {
            var (correlationPreds, otherPreds) = SplitPredicate(subquery.Predicate, correlationInfo.OuterColumns);

            foreach (var (outerColumn, innerColumn) in correlationPreds)
            {
                partitionColumns.Add(innerColumn);
            }

            remainingPredicate = otherPreds;

            // If the remaining predicate still contains any outer column references, we cannot
            // safely transform this subquery because those references would become invalid after
            // moving the subquery into a LEFT JOIN.
            if (remainingPredicate != null && ContainsAnyOuterColumnReference(remainingPredicate, correlationInfo.OuterColumns))
            {
                return null;
            }
        }

        // If we couldn't find partition columns from the predicate, try to infer from outer columns
        if (partitionColumns.Count == 0 && correlationInfo.OuterColumns.Count > 0)
        {
            // This is a fallback - the subquery correlates but we couldn't extract clean partition columns
            return null;
        }

        // Detect if the projected expression contains an aggregate function
        var containsAggregate = ContainsAggregateFunction(projectedExpression);

        // Generate a new alias for the subquery
        var subqueryAlias = _sqlAliasManager.GenerateTableAlias("subquery");

        // Build new projections and determine GROUP BY / ROW_NUMBER strategy
        var newProjections = new List<ProjectionExpression>();
        // Use a column name that won't conflict with BigQuery's special UNNEST "value" handling
        var valueColumnName = string.IsNullOrEmpty(projectedAlias) ? "_scalar_value" : projectedAlias;
        newProjections.Add(new ProjectionExpression(projectedExpression, valueColumnName));

        // Add partition columns (for the join condition)
        var partitionColumnNames = new List<string>();
        for (var i = 0; i < partitionColumns.Count; i++)
        {
            var partitionCol = partitionColumns[i];
            var partitionColName = $"_partition{i}";
            partitionColumnNames.Add(partitionColName);
            newProjections.Add(new ProjectionExpression(partitionCol, partitionColName));
        }

        IReadOnlyList<SqlExpression> groupByColumns;
        bool useRowNumber;

        if (containsAggregate)
        {
            // For aggregate subqueries (COUNT, SUM, etc.), use GROUP BY instead of ROW_NUMBER
            // The GROUP BY ensures one row per partition key, which is what we want
            groupByColumns = partitionColumns;
            useRowNumber = false;
        }
        else
        {
            // For non-aggregate subqueries with LIMIT 1, use ROW_NUMBER to pick one row per partition
            groupByColumns = subquery.GroupBy;
            useRowNumber = true;

            // Create the ROW_NUMBER expression
            var orderings = subquery.Orderings.Count > 0
                ? subquery.Orderings.ToList()
                : partitionColumns.Select(p => new OrderingExpression(p, ascending: true)).ToList();

            if (orderings.Count == 0)
            {
                // ROW_NUMBER requires at least one ordering
                orderings.Add(new OrderingExpression(
                    _sqlExpressionFactory.Constant(1, _typeMappingSource.FindMapping(typeof(int))),
                    ascending: true));
            }

            // Check if orderings contain any outer column references
            foreach (var ordering in orderings)
            {
                if (ContainsAnyOuterColumnReference(ordering.Expression, correlationInfo.OuterColumns))
                {
                    return null;
                }
            }

            var rowNumberExpression = new RowNumberExpression(
                partitionColumns,
                orderings,
                _typeMappingSource.FindMapping(typeof(long)));

            newProjections.Add(new ProjectionExpression(rowNumberExpression, "rn"));
        }

        // Create the inner SELECT
        var innerSelect = new SelectExpression(
            subqueryAlias,
            subquery.Tables.ToArray(),
            remainingPredicate,
            groupByColumns.ToArray(),
            subquery.Having,
            newProjections.ToArray(),
            distinct: false,
            orderings: Array.Empty<OrderingExpression>(),
            offset: null,
            limit: null);

        // Build the join predicate: outer.col = subq.partition_col (AND subq.rn = 1 for non-aggregate)
        SqlExpression? joinPredicate = null;

        if (subquery.Predicate != null)
        {
            var (correlationPreds, _) = SplitPredicate(subquery.Predicate, correlationInfo.OuterColumns);

            for (var i = 0; i < correlationPreds.Count; i++)
            {
                var (outerColumn, _) = correlationPreds[i];
                var partitionColumnRef = new ColumnExpression(
                    partitionColumnNames[i],
                    subqueryAlias,
                    outerColumn.Type,
                    outerColumn.TypeMapping,
                    nullable: true);

                var equalityPred = _sqlExpressionFactory.Equal(outerColumn, partitionColumnRef);
                joinPredicate = joinPredicate == null
                    ? equalityPred
                    : _sqlExpressionFactory.AndAlso(joinPredicate, equalityPred);
            }
        }

        // Add rn = 1 condition only for non-aggregate subqueries
        if (useRowNumber)
        {
            var rnColumn = new ColumnExpression(
                "rn",
                subqueryAlias,
                typeof(long),
                _typeMappingSource.FindMapping(typeof(long)),
                nullable: false);

            var rnCondition = _sqlExpressionFactory.Equal(
                rnColumn,
                _sqlExpressionFactory.Constant(1L, _typeMappingSource.FindMapping(typeof(long))));

            joinPredicate = joinPredicate == null
                ? rnCondition
                : _sqlExpressionFactory.AndAlso(joinPredicate, rnCondition);
        }

        // Create the LEFT JOIN
        var leftJoin = new LeftJoinExpression(innerSelect, joinPredicate!, prunable: false);

        // Create the column reference that will replace the scalar subquery
        var replacementColumn = new ColumnExpression(
            valueColumnName,
            subqueryAlias,
            scalarSubquery.Type,
            scalarSubquery.TypeMapping,
            nullable: true);

        // Record the join to add
        _joinsToAdd ??= new List<LeftJoinExpression>();
        _joinsToAdd.Add(leftJoin);

        // For aggregate expressions, wrap in COALESCE to handle NULL when no rows match
        // This preserves the semantics of COUNT returning 0 instead of NULL
        SqlExpression result = replacementColumn;
        if (containsAggregate)
        {
            var defaultValue = GetAggregateDefaultValue(scalarSubquery.Type, scalarSubquery.TypeMapping);
            if (defaultValue != null)
            {
                result = _sqlExpressionFactory.Coalesce(replacementColumn, defaultValue);
            }
        }

        return result;
    }

    /// <summary>
    /// Gets the default value for an aggregate expression (used in COALESCE when LEFT JOIN returns NULL).
    /// </summary>
    private SqlExpression? GetAggregateDefaultValue(Type type, RelationalTypeMapping? typeMapping)
    {
        // For numeric types, return 0
        if (type == typeof(int) || type == typeof(int?))
        {
            return _sqlExpressionFactory.Constant(0, typeMapping ?? _typeMappingSource.FindMapping(typeof(int)));
        }
        if (type == typeof(long) || type == typeof(long?))
        {
            return _sqlExpressionFactory.Constant(0L, typeMapping ?? _typeMappingSource.FindMapping(typeof(long)));
        }
        if (type == typeof(decimal) || type == typeof(decimal?))
        {
            return _sqlExpressionFactory.Constant(0m, typeMapping ?? _typeMappingSource.FindMapping(typeof(decimal)));
        }
        if (type == typeof(double) || type == typeof(double?))
        {
            return _sqlExpressionFactory.Constant(0d, typeMapping ?? _typeMappingSource.FindMapping(typeof(double)));
        }
        if (type == typeof(float) || type == typeof(float?))
        {
            return _sqlExpressionFactory.Constant(0f, typeMapping ?? _typeMappingSource.FindMapping(typeof(float)));
        }

        // For other types, don't add COALESCE (let NULL propagate)
        return null;
    }

    /// <summary>
    /// Checks if an expression contains an aggregate function (COUNT, SUM, AVG, MIN, MAX, etc.).
    /// </summary>
    private static bool ContainsAggregateFunction(SqlExpression expression)
    {
        var finder = new AggregateFunctionFinder();
        finder.Visit(expression);
        return finder.Found;
    }

    private (List<(ColumnExpression outer, SqlExpression inner)> correlationPreds, SqlExpression? remaining)
        SplitPredicate(SqlExpression predicate, HashSet<ColumnExpression> outerColumns)
    {
        var correlationPreds = new List<(ColumnExpression, SqlExpression)>();
        var remainingPreds = new List<SqlExpression>();

        SplitPredicateRecursive(predicate, outerColumns, correlationPreds, remainingPreds);

        SqlExpression? remaining = null;
        foreach (var pred in remainingPreds)
        {
            remaining = remaining == null
                ? pred
                : _sqlExpressionFactory.AndAlso(remaining, pred);
        }

        return (correlationPreds, remaining);
    }

    private void SplitPredicateRecursive(
        SqlExpression predicate,
        HashSet<ColumnExpression> outerColumns,
        List<(ColumnExpression, SqlExpression)> correlationPreds,
        List<SqlExpression> remainingPreds)
    {
        if (predicate is SqlBinaryExpression { OperatorType: ExpressionType.AndAlso } andExpr)
        {
            SplitPredicateRecursive(andExpr.Left, outerColumns, correlationPreds, remainingPreds);
            SplitPredicateRecursive(andExpr.Right, outerColumns, correlationPreds, remainingPreds);
            return;
        }

        if (predicate is SqlBinaryExpression { OperatorType: ExpressionType.Equal } eqExpr)
        {
            // Check if one side is an outer column and the other is an inner expression
            if (eqExpr.Left is ColumnExpression leftCol && outerColumns.Contains(leftCol))
            {
                correlationPreds.Add((leftCol, eqExpr.Right));
                return;
            }

            if (eqExpr.Right is ColumnExpression rightCol && outerColumns.Contains(rightCol))
            {
                correlationPreds.Add((rightCol, eqExpr.Left));
                return;
            }
        }

        // Check if this predicate references any outer columns
        var referencesOuter = ContainsOuterColumn(predicate, outerColumns);
        if (!referencesOuter)
        {
            remainingPreds.Add(predicate);
        }
        // If it references outer columns but isn't a simple equality, we skip it
        // (it will be lost in the transformation - complex case)
    }

    private bool ContainsOuterColumn(SqlExpression expression, HashSet<ColumnExpression> outerColumns)
    {
        var finder = new SpecificColumnFinder(outerColumns);
        finder.Visit(expression);
        return finder.Found;
    }

    /// <summary>
    /// Checks if an expression contains nested scalar subqueries that still reference outer columns.
    /// If so, transforming the parent subquery to a LEFT JOIN would break these nested references.
    /// </summary>
    private bool ContainsNestedCorrelatedSubquery(SqlExpression expression, HashSet<ColumnExpression> outerColumns)
    {
        var finder = new NestedCorrelatedSubqueryFinder(outerColumns);
        finder.Visit(expression);
        return finder.Found;
    }

    /// <summary>
    /// Checks if an expression contains any reference to outer columns.
    /// This is used to detect cases where parts of the subquery (predicate, orderings, projection)
    /// still reference outer tables after we've extracted the correlation predicate.
    /// </summary>
    private bool ContainsAnyOuterColumnReference(SqlExpression expression, HashSet<ColumnExpression> outerColumns)
    {
        var finder = new OuterColumnReferenceFinder(outerColumns);
        finder.Visit(expression);
        return finder.Found;
    }

    private static HashSet<string> CollectTableAliases(SelectExpression select)
    {
        var aliases = new HashSet<string>();
        foreach (var table in select.Tables)
        {
            var alias = table.UnwrapJoin().Alias;
            if (alias != null)
            {
                aliases.Add(alias);
            }
        }
        return aliases;
    }

    private class OuterColumnFinder : ExpressionVisitor
    {
        private readonly HashSet<string> _outerTableAliases;

        public HashSet<ColumnExpression> OuterColumns { get; } = new();
        public List<SqlBinaryExpression> CorrelationPredicates { get; } = new();

        public OuterColumnFinder(HashSet<string> outerTableAliases)
        {
            _outerTableAliases = outerTableAliases;
        }

        protected override Expression VisitExtension(Expression node)
        {
            if (node is ColumnExpression column && _outerTableAliases.Contains(column.TableAlias))
            {
                OuterColumns.Add(column);
            }

            return base.VisitExtension(node);
        }
    }

    private class SpecificColumnFinder : ExpressionVisitor
    {
        private readonly HashSet<ColumnExpression> _targetColumns;

        public bool Found { get; private set; }

        public SpecificColumnFinder(HashSet<ColumnExpression> targetColumns)
        {
            _targetColumns = targetColumns;
        }

        protected override Expression VisitExtension(Expression node)
        {
            if (node is ColumnExpression column && _targetColumns.Contains(column))
            {
                Found = true;
                return node;
            }

            return base.VisitExtension(node);
        }
    }

    /// <summary>
    /// Finds nested scalar subqueries that reference outer columns.
    /// This is used to detect cases where transforming the parent subquery would break nested references.
    /// </summary>
    private class NestedCorrelatedSubqueryFinder : ExpressionVisitor
    {
        private readonly HashSet<ColumnExpression> _outerColumns;

        public bool Found { get; private set; }

        public NestedCorrelatedSubqueryFinder(HashSet<ColumnExpression> outerColumns)
        {
            _outerColumns = outerColumns;
        }

        protected override Expression VisitExtension(Expression node)
        {
            if (Found)
            {
                return node; // Short-circuit if already found
            }

            if (node is ScalarSubqueryExpression scalarSubquery)
            {
                // Check if this nested subquery references any outer columns
                var columnFinder = new SpecificColumnFinder(_outerColumns);
                columnFinder.Visit(scalarSubquery);
                if (columnFinder.Found)
                {
                    Found = true;
                    return node;
                }
            }

            return base.VisitExtension(node);
        }
    }

    /// <summary>
    /// Detects if an expression contains aggregate functions (COUNT, SUM, AVG, MIN, MAX, etc.).
    /// </summary>
    private class AggregateFunctionFinder : ExpressionVisitor
    {
        private static readonly HashSet<string> AggregateNames = new(StringComparer.OrdinalIgnoreCase)
        {
            "COUNT", "SUM", "AVG", "MIN", "MAX", "COUNT_BIG",
            "STDEV", "STDEVP", "VAR", "VARP",
            "STRING_AGG", "ARRAY_AGG", "ANY_VALUE",
            "BIT_AND", "BIT_OR", "BIT_XOR",
            "APPROX_COUNT_DISTINCT", "APPROX_QUANTILES", "APPROX_TOP_COUNT", "APPROX_TOP_SUM",
            "CORR", "COVAR_POP", "COVAR_SAMP",
            "COUNTIF", "LOGICAL_AND", "LOGICAL_OR"
        };

        public bool Found { get; private set; }

        protected override Expression VisitExtension(Expression node)
        {
            if (Found)
            {
                return node;
            }

            if (node is SqlFunctionExpression function && AggregateNames.Contains(function.Name))
            {
                Found = true;
                return node;
            }

            // Also check for AggregateSqlExpression if EF Core uses it
            if (node.GetType().Name.Contains("Aggregate", StringComparison.OrdinalIgnoreCase))
            {
                Found = true;
                return node;
            }

            return base.VisitExtension(node);
        }
    }

    /// <summary>
    /// Finds any reference to outer columns within an expression.
    /// Used to determine if an expression can safely be moved into a JOIN.
    /// </summary>
    private class OuterColumnReferenceFinder : ExpressionVisitor
    {
        private readonly HashSet<ColumnExpression> _outerColumns;
        private readonly HashSet<string> _outerTableAliases;

        public bool Found { get; private set; }

        public OuterColumnReferenceFinder(HashSet<ColumnExpression> outerColumns)
        {
            _outerColumns = outerColumns;
            _outerTableAliases = outerColumns.Select(c => c.TableAlias).ToHashSet();
        }

        protected override Expression VisitExtension(Expression node)
        {
            if (Found)
            {
                return node; // Short-circuit if already found
            }

            if (node is ColumnExpression column)
            {
                // Check if this column references an outer table
                if (_outerTableAliases.Contains(column.TableAlias))
                {
                    Found = true;
                    return node;
                }
            }

            return base.VisitExtension(node);
        }
    }

    private class CorrelationInfo
    {
        public bool IsCorrelated { get; init; }
        public HashSet<ColumnExpression> OuterColumns { get; init; } = new();
        public List<SqlBinaryExpression> CorrelationPredicates { get; init; } = new();
    }
}
