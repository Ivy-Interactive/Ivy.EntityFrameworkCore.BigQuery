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

        // Process tables one by one, inserting any new JOINs before the table that references them
        var newTables = new List<TableExpressionBase>();
        var tablesChanged = false;

        foreach (var table in select.Tables)
        {
            // Clear _joinsToAdd before processing each table
            _joinsToAdd = null;

            // Visit the table (this will find scalar subqueries in ON clauses)
            var visitedTable = VisitTableExpression(table);

            // If we found scalar subqueries in this table's ON clause, insert their JOINs BEFORE this table
            if (_joinsToAdd is not null && _joinsToAdd.Count > 0)
            {
                newTables.AddRange(_joinsToAdd);
                tablesChanged = true;
            }

            newTables.Add(visitedTable);
            if (visitedTable != table)
            {
                tablesChanged = true;
            }

            // Add this table's alias to outer aliases for subsequent processing
            var tableAlias = visitedTable.UnwrapJoin().Alias;
            if (tableAlias != null)
            {
                _outerTableAliases!.Add(tableAlias);
            }
            // Also add any new JOIN aliases
            if (_joinsToAdd is not null)
            {
                foreach (var join in _joinsToAdd)
                {
                    var joinAlias = join.UnwrapJoin().Alias;
                    if (joinAlias != null)
                    {
                        _outerTableAliases!.Add(joinAlias);
                    }
                }
            }
        }

        // Clear _joinsToAdd and process non-table parts of the SELECT
        _joinsToAdd = null;

        // Visit projections, predicate, orderings, etc.
        var newProjections = select.Projection.Select(p => (ProjectionExpression)Visit(p)).ToList();
        var newPredicate = select.Predicate is not null ? (SqlExpression?)Visit(select.Predicate) : null;
        var newGroupBy = select.GroupBy.Select(g => (SqlExpression)Visit(g)).ToList();
        var newHaving = select.Having is not null ? (SqlExpression?)Visit(select.Having) : null;
        var newOrderings = select.Orderings.Select(o =>
            new OrderingExpression((SqlExpression)Visit(o.Expression), o.IsAscending)).ToList();
        var newOffset = select.Offset is not null ? (SqlExpression?)Visit(select.Offset) : null;
        var newLimit = select.Limit is not null ? (SqlExpression?)Visit(select.Limit) : null;

        // Check for any additional JOINs from non-table parts (projections, predicate, etc.)
        if (_joinsToAdd is not null && _joinsToAdd.Count > 0)
        {
            // Add these at the end since they came from projections/predicates, not table ON clauses
            newTables.AddRange(_joinsToAdd);
            tablesChanged = true;
        }

        // Determine if anything changed
        var projectionsChanged = !select.Projection.SequenceEqual(newProjections);
        var predicateChanged = select.Predicate != newPredicate;
        var groupByChanged = !select.GroupBy.SequenceEqual(newGroupBy);
        var havingChanged = select.Having != newHaving;
        var orderingsChanged = !select.Orderings.SequenceEqual(newOrderings);
        var offsetChanged = select.Offset != newOffset;
        var limitChanged = select.Limit != newLimit;

        SelectExpression visitedSelect;
        if (tablesChanged || projectionsChanged || predicateChanged || groupByChanged ||
            havingChanged || orderingsChanged || offsetChanged || limitChanged)
        {
            visitedSelect = select.Update(
                tablesChanged ? newTables : select.Tables,
                predicateChanged ? newPredicate : select.Predicate,
                groupByChanged ? newGroupBy : select.GroupBy,
                havingChanged ? newHaving : select.Having,
                projectionsChanged ? newProjections : select.Projection,
                orderingsChanged ? newOrderings : select.Orderings,
                offsetChanged ? newOffset : select.Offset,
                limitChanged ? newLimit : select.Limit);
        }
        else
        {
            visitedSelect = select;
        }

        (_currentSelect, _joinsToAdd, _outerTableAliases) =
            (parentSelect, parentJoinsToAdd, parentOuterAliases);

        return visitedSelect;
    }

    private TableExpressionBase VisitTableExpression(TableExpressionBase table)
    {
        return table switch
        {
            LeftJoinExpression leftJoin => VisitLeftJoin(leftJoin),
            InnerJoinExpression innerJoin => VisitInnerJoin(innerJoin),
            CrossJoinExpression crossJoin => VisitCrossJoin(crossJoin),
            SelectExpression nestedSelect => (TableExpressionBase)VisitSelect(nestedSelect),
            _ => table
        };
    }

    private LeftJoinExpression VisitLeftJoin(LeftJoinExpression leftJoin)
    {
        var visitedTable = VisitTableExpression(leftJoin.Table);
        var visitedPredicate = (SqlExpression)Visit(leftJoin.JoinPredicate);

        if (visitedTable != leftJoin.Table || visitedPredicate != leftJoin.JoinPredicate)
        {
            return new LeftJoinExpression(visitedTable, visitedPredicate, leftJoin.IsPrunable);
        }
        return leftJoin;
    }

    private InnerJoinExpression VisitInnerJoin(InnerJoinExpression innerJoin)
    {
        var visitedTable = VisitTableExpression(innerJoin.Table);
        var visitedPredicate = (SqlExpression)Visit(innerJoin.JoinPredicate);

        if (visitedTable != innerJoin.Table || visitedPredicate != innerJoin.JoinPredicate)
        {
            return new InnerJoinExpression(visitedTable, visitedPredicate, innerJoin.IsPrunable);
        }
        return innerJoin;
    }

    private CrossJoinExpression VisitCrossJoin(CrossJoinExpression crossJoin)
    {
        var visitedTable = VisitTableExpression(crossJoin.Table);

        if (visitedTable != crossJoin.Table)
        {
            return new CrossJoinExpression(visitedTable);
        }
        return crossJoin;
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

        // If we couldn't find partition columns from the direct predicate, try to find them in nested FROM tables
        List<(ColumnExpression outer, SqlExpression inner)>? nestedCorrelationPreds = null;
        SelectExpression? nestedTableWithCorrelation = null;
        int nestedTableIndex = -1;

        if (partitionColumns.Count == 0 && correlationInfo.OuterColumns.Count > 0)
        {
            // Look for correlation in nested FROM tables (e.g., after DISTINCT)
            var nestedCorrelation = ExtractCorrelationFromNestedTables(subquery, correlationInfo.OuterColumns);
            if (nestedCorrelation != null)
            {
                nestedCorrelationPreds = nestedCorrelation.Value.correlationPreds;
                nestedTableWithCorrelation = nestedCorrelation.Value.nestedTable;
                nestedTableIndex = nestedCorrelation.Value.tableIndex;

                foreach (var (outerColumn, innerColumn) in nestedCorrelationPreds)
                {
                    partitionColumns.Add(innerColumn);
                }
            }
        }

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
            // Try orderings from: 1) main subquery, 2) nested table with correlation, 3) partition columns
            List<OrderingExpression> orderings;
            if (subquery.Orderings.Count > 0)
            {
                orderings = subquery.Orderings.ToList();
            }
            else if (nestedTableWithCorrelation?.Orderings.Count > 0)
            {
                // The orderings are in the nested table - we need to remap column references
                // to point to the nested table's alias (since they'll be in the outer SELECT now)
                orderings = nestedTableWithCorrelation.Orderings
                    .Select(o => RemapOrderingToNestedAlias(o, nestedTableWithCorrelation))
                    .ToList();
            }
            else
            {
                orderings = partitionColumns.Select(p => new OrderingExpression(p, ascending: true)).ToList();
            }

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

        // Prepare the tables for the inner SELECT
        // When using ROW_NUMBER, we need to clear LIMIT/OFFSET/Orderings from nested tables:
        // - LIMIT is now handled by rn = 1 in the outer join condition
        // - Orderings are now in the ROW_NUMBER's ORDER BY clause
        // - OFFSET would need different handling (we'll clear it too)
        var tables = subquery.Tables.ToList();

        if (useRowNumber)
        {
            // Clear LIMIT/OFFSET from ALL nested SelectExpressions
            tables = ClearLimitFromNestedTables(tables);
        }

        if (nestedTableWithCorrelation != null && nestedTableIndex >= 0)
        {
            // Rebuild the nested table without the correlation predicate
            var (_, otherPreds) = SplitPredicate(nestedTableWithCorrelation.Predicate!, correlationInfo.OuterColumns);

            var newNestedTable = nestedTableWithCorrelation.Update(
                useRowNumber ? ClearLimitFromNestedTables(nestedTableWithCorrelation.Tables.ToList()) : nestedTableWithCorrelation.Tables,
                otherPreds,  // Remove correlation predicates
                nestedTableWithCorrelation.GroupBy,
                nestedTableWithCorrelation.Having,
                nestedTableWithCorrelation.Projection,
                useRowNumber ? [] : nestedTableWithCorrelation.Orderings,  // Clear orderings when using ROW_NUMBER
                useRowNumber ? null : nestedTableWithCorrelation.Offset,   // Clear offset when using ROW_NUMBER
                useRowNumber ? null : nestedTableWithCorrelation.Limit);   // Clear limit when using ROW_NUMBER

            // Replace the table in the list
            tables[nestedTableIndex] = newNestedTable;
        }

        // Create the inner SELECT
        var innerSelect = new SelectExpression(
            subqueryAlias,
            tables.ToArray(),
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

        // Use nested correlation predicates if available, otherwise use direct predicates
        var correlationPredsForJoin = nestedCorrelationPreds ?? (subquery.Predicate != null
            ? SplitPredicate(subquery.Predicate, correlationInfo.OuterColumns).Item1
            : new List<(ColumnExpression, SqlExpression)>());

        for (var i = 0; i < correlationPredsForJoin.Count; i++)
        {
            var (outerColumn, _) = correlationPredsForJoin[i];
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

        // For COUNT and SUM aggregates, wrap in COALESCE to return 0 instead of NULL when no rows match.
        // This matches EF Core's expected semantics for navigation property aggregates.
        // MIN, MAX, AVG should return NULL for empty sets - that's the expected behavior.
        SqlExpression result = replacementColumn;
        if (ContainsCountOrSumAggregate(projectedExpression))
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
    /// Checks if an expression contains a COUNT or SUM aggregate function.
    /// COUNT and SUM should get COALESCE with 0, as they have the defined semantic of returning 0 for empty sets.
    /// MIN, MAX, AVG should return NULL for empty sets.
    /// </summary>
    private static bool ContainsCountOrSumAggregate(SqlExpression expression)
    {
        var finder = new CountOrSumAggregateFinder();
        finder.Visit(expression);
        return finder.Found;
    }

    /// <summary>
    /// Gets the default value (0) for COUNT or SUM aggregates.
    /// </summary>
    private SqlExpression? GetAggregateDefaultValue(Type type, RelationalTypeMapping? typeMapping)
    {
        // Integer types (COUNT, SUM of integers)
        if (type == typeof(int) || type == typeof(int?))
        {
            return _sqlExpressionFactory.Constant(0, typeMapping ?? _typeMappingSource.FindMapping(typeof(int)));
        }
        if (type == typeof(long) || type == typeof(long?))
        {
            return _sqlExpressionFactory.Constant(0L, typeMapping ?? _typeMappingSource.FindMapping(typeof(long)));
        }
        // Decimal types (SUM of decimals)
        if (type == typeof(decimal) || type == typeof(decimal?))
        {
            return _sqlExpressionFactory.Constant(0m, typeMapping ?? _typeMappingSource.FindMapping(typeof(decimal)));
        }
        // Floating point types (SUM of floats/doubles)
        if (type == typeof(double) || type == typeof(double?))
        {
            return _sqlExpressionFactory.Constant(0d, typeMapping ?? _typeMappingSource.FindMapping(typeof(double)));
        }
        if (type == typeof(float) || type == typeof(float?))
        {
            return _sqlExpressionFactory.Constant(0f, typeMapping ?? _typeMappingSource.FindMapping(typeof(float)));
        }

        // For other types, don't add COALESCE
        return null;
    }

    /// <summary>
    /// Clears LIMIT and Orderings from all nested SelectExpressions in a table list.
    /// This is needed when using ROW_NUMBER transformation since the per-partition limiting
    /// is now handled by rn = 1 instead of LIMIT.
    /// Note: OFFSET is preserved as it requires special handling in the ROW_NUMBER filter.
    /// </summary>
    private List<TableExpressionBase> ClearLimitFromNestedTables(IEnumerable<TableExpressionBase> tables)
    {
        var result = new List<TableExpressionBase>();
        foreach (var table in tables)
        {
            var tableToProcess = table.UnwrapJoin();
            if (tableToProcess is SelectExpression nestedSelect)
            {
                // Check if this SELECT needs LIMIT/Orderings cleared
                // Note: We preserve OFFSET as it needs special handling (rn > offset condition)
                var needsClear = nestedSelect.Limit != null || nestedSelect.Orderings.Count > 0;

                // Always recurse to handle deeply nested SelectExpressions
                var clearedNestedTables = ClearLimitFromNestedTables(nestedSelect.Tables.ToList());
                var nestedTablesChanged = !clearedNestedTables.SequenceEqual(nestedSelect.Tables);

                if (needsClear || nestedTablesChanged)
                {
                    // Clear LIMIT and Orderings from this SELECT if needed (preserve OFFSET)
                    var clearedSelect = nestedSelect.Update(
                        clearedNestedTables,
                        nestedSelect.Predicate,
                        nestedSelect.GroupBy,
                        nestedSelect.Having,
                        nestedSelect.Projection,
                        needsClear ? [] : nestedSelect.Orderings,
                        nestedSelect.Offset,  // Preserve OFFSET - needs special handling
                        needsClear ? null : nestedSelect.Limit);

                    // Preserve the join wrapper if present
                    if (table is LeftJoinExpression leftJoin)
                    {
                        result.Add(new LeftJoinExpression(clearedSelect, leftJoin.JoinPredicate, leftJoin.IsPrunable));
                    }
                    else if (table is InnerJoinExpression innerJoin)
                    {
                        result.Add(new InnerJoinExpression(clearedSelect, innerJoin.JoinPredicate, innerJoin.IsPrunable));
                    }
                    else if (table is CrossJoinExpression)
                    {
                        result.Add(new CrossJoinExpression(clearedSelect));
                    }
                    else
                    {
                        result.Add(clearedSelect);
                    }
                }
                else
                {
                    result.Add(table);
                }
            }
            else
            {
                result.Add(table);
            }
        }
        return result;
    }

    /// <summary>
    /// Remaps an ordering expression from the nested table's column references to
    /// use the nested table's alias (so they can be used in the outer SELECT's ROW_NUMBER).
    /// For example, if the nested table has alias "o0" and contains ORDER BY o.OrderID,
    /// we need to reference o0.OrderID which is projected by the nested select.
    /// </summary>
    private OrderingExpression RemapOrderingToNestedAlias(OrderingExpression ordering, SelectExpression nestedTable)
    {
        if (ordering.Expression is ColumnExpression col)
        {
            // Find the projection in the nested table that contains this column
            foreach (var projection in nestedTable.Projection)
            {
                if (projection.Expression is ColumnExpression projCol &&
                    projCol.TableAlias == col.TableAlias &&
                    projCol.Name == col.Name)
                {
                    // Create a column reference to the nested table's projection
                    var remappedCol = new ColumnExpression(
                        projection.Alias,
                        nestedTable.Alias!,
                        col.Type,
                        col.TypeMapping,
                        col.IsNullable);
                    return new OrderingExpression(remappedCol, ordering.IsAscending);
                }
            }
        }
        // If we can't remap, return as-is (might still work if column is directly accessible)
        return ordering;
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

    /// <summary>
    /// Looks for correlation predicates inside nested FROM tables (SelectExpressions).
    /// This handles cases like: SELECT ... FROM (SELECT ... FROM t WHERE outer.col = t.col) AS nested LIMIT 1
    /// where the correlation is inside the nested SELECT rather than the outer one.
    /// </summary>
    private (List<(ColumnExpression outer, SqlExpression inner)> correlationPreds, SelectExpression nestedTable, int tableIndex)?
        ExtractCorrelationFromNestedTables(SelectExpression subquery, HashSet<ColumnExpression> outerColumns)
    {
        for (var i = 0; i < subquery.Tables.Count; i++)
        {
            var table = subquery.Tables[i].UnwrapJoin();

            if (table is SelectExpression nestedSelect && nestedSelect.Predicate != null)
            {
                var (correlationPreds, otherPreds) = SplitPredicate(nestedSelect.Predicate, outerColumns);

                if (correlationPreds.Count > 0)
                {
                    // Check if the remaining predicate has any outer column references
                    if (otherPreds != null && ContainsAnyOuterColumnReference(otherPreds, outerColumns))
                    {
                        // Complex case - skip this nested table
                        continue;
                    }

                    // We need to remap the inner columns from the nested table to the outer subquery's perspective
                    // For example, if nested table has alias "w" and correlation is "outer.col = w.inner_col",
                    // we need to create a column reference to "nestedAlias.inner_col" for the outer subquery.
                    var remappedCorrelationPreds = new List<(ColumnExpression outer, SqlExpression inner)>();

                    foreach (var (outerCol, innerExpr) in correlationPreds)
                    {
                        if (innerExpr is ColumnExpression innerCol)
                        {
                            // Find the projection in the nested select that exposes this column
                            // The nested select might project this column with the same or different name
                            var projectedColumn = FindProjectedColumn(nestedSelect, innerCol, table.Alias!);
                            if (projectedColumn != null)
                            {
                                remappedCorrelationPreds.Add((outerCol, projectedColumn));
                            }
                            else
                            {
                                // The inner column is not projected by the nested select - can't use it
                                // This can happen if the correlation column isn't in the SELECT list
                                continue;
                            }
                        }
                        else
                        {
                            // Complex inner expression - not supported yet
                            continue;
                        }
                    }

                    if (remappedCorrelationPreds.Count > 0)
                    {
                        return (remappedCorrelationPreds, nestedSelect, i);
                    }
                }
            }
        }

        return null;
    }

    /// <summary>
    /// Finds a column in the nested select's projection that corresponds to the given column.
    /// Returns a ColumnExpression referencing the nested table's alias.
    /// </summary>
    private ColumnExpression? FindProjectedColumn(SelectExpression nestedSelect, ColumnExpression innerColumn, string nestedAlias)
    {
        foreach (var projection in nestedSelect.Projection)
        {
            if (projection.Expression is ColumnExpression projectedCol &&
                projectedCol.TableAlias == innerColumn.TableAlias &&
                projectedCol.Name == innerColumn.Name)
            {
                // Found the column in the projection - create a reference to it through the nested alias
                return new ColumnExpression(
                    projection.Alias,
                    nestedAlias,
                    innerColumn.Type,
                    innerColumn.TypeMapping,
                    nullable: innerColumn.IsNullable);
            }
        }

        return null;
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
    /// Finds COUNT and SUM aggregate functions.
    /// COUNT and SUM should have COALESCE with 0, as they have the defined semantic
    /// of returning 0 for empty sets in EF Core's navigation property context.
    /// MIN, MAX, AVG should return NULL for empty sets.
    /// </summary>
    private class CountOrSumAggregateFinder : ExpressionVisitor
    {
        private static readonly HashSet<string> AggregateNames = new(StringComparer.OrdinalIgnoreCase)
        {
            "COUNT", "COUNT_BIG", "COUNTIF", "SUM"
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
