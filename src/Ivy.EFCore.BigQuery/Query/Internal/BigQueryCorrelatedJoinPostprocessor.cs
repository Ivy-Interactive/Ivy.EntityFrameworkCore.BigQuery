using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
using System.Linq.Expressions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal;

#pragma warning disable EF1001 // Internal EF Core API usage.

/// <summary>
/// Handles correlated predicates in JOINs that reference outer tables.
/// BigQuery doesn't support correlated subqueries inside FROM clause JOINs.
/// This postprocessor extracts correlated predicates from inner subqueries and moves them
/// to the outer JOIN condition.
///
/// For example:
/// LEFT JOIN (
///     SELECT ... FROM ... INNER JOIN (
///         SELECT ... FROM ... WHERE outer.Column = inner.Column  -- Correlated!
///     ) ...
/// ) AS s ON ...
///
/// Becomes:
/// LEFT JOIN (
///     SELECT ..., inner.Column AS _corr_col FROM ... INNER JOIN (
///         SELECT ... FROM ...
///     ) ...
/// ) AS s ON ... AND outer.Column = s._corr_col
/// </summary>
public class BigQueryCorrelatedJoinPostprocessor : ExpressionVisitor
{
    private readonly ISqlExpressionFactory _sqlExpressionFactory;
    private readonly IRelationalTypeMappingSource _typeMappingSource;

    // Track table aliases from ancestor SELECTs (need to be lifted out completely)
    private HashSet<string> _ancestorAliases = new();

    // Track table aliases that are "outer" from nested perspective (ancestors + siblings)
    private HashSet<string> _outerTableAliases = new();

    // Track correlated projection remappings: (joinAlias, projectionAlias) -> outer expression
    // When we remove correlated projections from an inner SELECT, outer references need remapping
    // This can be either a simple ColumnExpression or a complex SqlExpression (e.g., c.CustomerID + c.City)
    private Dictionary<(string tableAlias, string columnName), SqlExpression> _correlatedProjectionRemappings = new();

    public BigQueryCorrelatedJoinPostprocessor(
        ISqlExpressionFactory sqlExpressionFactory,
        IRelationalTypeMappingSource typeMappingSource)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
        _typeMappingSource = typeMappingSource;
    }

    protected override Expression VisitExtension(Expression node)
    {
        DebugLog($"VisitExtension: {node.GetType().Name}");
        return node switch
        {
            ShapedQueryExpression shapedQuery => shapedQuery
                .UpdateQueryExpression(Visit(shapedQuery.QueryExpression))
                .UpdateShaperExpression(Visit(shapedQuery.ShaperExpression)),
            SelectExpression select => VisitSelect(select),
            _ => base.VisitExtension(node)
        };
    }

    private Expression VisitSelect(SelectExpression select)
    {
        // Save current state
        var previousAncestorAliases = _ancestorAliases;
        var previousOuterAliases = _outerTableAliases;
        var previousRemappings = _correlatedProjectionRemappings;

        // For nested SELECTs, current outer aliases become ancestors
        _ancestorAliases = new HashSet<string>(previousOuterAliases);
        _outerTableAliases = new HashSet<string>(previousOuterAliases);
        _correlatedProjectionRemappings = new Dictionary<(string, string), SqlExpression>();

        // Process tables and transform correlated JOINs
        // Add each table's alias to _outerTableAliases AFTER processing it
        // (so sibling tables that come later can reference it as "outer")
        var newTables = new List<TableExpressionBase>();
        var tablesChanged = false;

        foreach (var table in select.Tables)
        {
            var tableAlias = table.UnwrapJoin().Alias;

            var newTable = VisitTableExpression(table);
            newTables.Add(newTable);

            if (newTable != table)
            {
                tablesChanged = true;
            }

            // Add this table's alias to outer aliases for subsequent tables
            if (tableAlias != null)
            {
                _outerTableAliases.Add(tableAlias);
            }
        }

        // Apply correlated projection remappings to this SELECT's projections, predicates, etc.
        var remapper = _correlatedProjectionRemappings.Count > 0
            ? new CorrelatedProjectionRemapper(_correlatedProjectionRemappings)
            : null;

        var newProjections = select.Projection;
        var newPredicate = select.Predicate;
        var newGroupBy = select.GroupBy;
        var newHaving = select.Having;
        var newOrderings = select.Orderings;

        if (remapper != null)
        {
            DebugLog($"  Applying {_correlatedProjectionRemappings.Count} remappings to outer SELECT");

            newProjections = select.Projection.Select(p =>
            {
                var remapped = (ProjectionExpression)remapper.Visit(p);
                if (remapped != p)
                    DebugLog($"    Remapped projection: {p.Alias}");
                return remapped;
            }).ToList();

            if (newPredicate != null)
                newPredicate = (SqlExpression)remapper.Visit(newPredicate);

            newGroupBy = select.GroupBy.Select(g => (SqlExpression)remapper.Visit(g)).ToList();

            if (newHaving != null)
                newHaving = (SqlExpression)remapper.Visit(newHaving);

            newOrderings = select.Orderings.Select(o => (OrderingExpression)remapper.Visit(o)).ToList();
        }

        _ancestorAliases = previousAncestorAliases;
        _outerTableAliases = previousOuterAliases;
        _correlatedProjectionRemappings = previousRemappings;

        var projectionsChanged = remapper != null && !newProjections.SequenceEqual(select.Projection);
        var groupByChanged = remapper != null && !newGroupBy.SequenceEqual(select.GroupBy);
        var orderingsChanged = remapper != null && !newOrderings.SequenceEqual(select.Orderings);

        if (tablesChanged || projectionsChanged || newPredicate != select.Predicate ||
            newHaving != select.Having || groupByChanged || orderingsChanged)
        {
            return select.Update(
                newTables,
                newPredicate,
                newGroupBy,
                newHaving,
                newProjections,
                newOrderings,
                select.Offset,
                select.Limit);
        }

        return select;
    }

    /// <summary>
    /// Remaps column references from join table aliases to outer expressions.
    /// This handles both simple column remappings (e.g., o0.CustomerID -> c.CustomerID)
    /// and complex expression remappings (e.g., o0.Complex -> c.CustomerID + c.City).
    /// </summary>
    private class CorrelatedProjectionRemapper : ExpressionVisitor
    {
        private readonly Dictionary<(string tableAlias, string columnName), SqlExpression> _remappings;

        public CorrelatedProjectionRemapper(Dictionary<(string, string), SqlExpression> remappings)
        {
            _remappings = remappings;
        }

        protected override Expression VisitExtension(Expression node)
        {
            if (node is ColumnExpression column)
            {
                var key = (column.TableAlias, column.Name);
                if (_remappings.TryGetValue(key, out var outerExpression))
                {
                    return outerExpression;
                }
            }

            return base.VisitExtension(node);
        }
    }

    private static readonly object _logLock = new object();
    private static void DebugLog(string msg)
    {
        try
        {
            lock (_logLock)
            {
                System.IO.File.AppendAllText(@"D:\Repos\Ivy.EntityFrameworkCore.BigQuery\bq_debug.txt", $"{DateTime.Now:HH:mm:ss.fff} {msg}\n");
            }
        }
        catch { }
    }

    private static string DebugPrintExpression(Expression? expr, int depth = 0)
    {
        if (expr == null) return "null";
        var indent = new string(' ', depth * 2);
        return expr switch
        {
            SqlBinaryExpression bin => $"{bin.OperatorType}(\n{indent}  L: {DebugPrintExpression(bin.Left, depth + 1)}\n{indent}  R: {DebugPrintExpression(bin.Right, depth + 1)})",
            ColumnExpression col => $"Column({col.TableAlias}.{col.Name})",
            SqlConstantExpression c => $"Const({c.Value})",
            SqlUnaryExpression un => $"{un.OperatorType}({DebugPrintExpression(un.Operand, depth + 1)})",
            _ => $"{expr.GetType().Name}"
        };
    }

    private TableExpressionBase VisitTableExpression(TableExpressionBase table)
    {
        DebugLog($"VisitTableExpression: {table.GetType().Name}, alias={table.UnwrapJoin().Alias}");
        return table switch
        {
            LeftJoinExpression leftJoin => VisitLeftJoin(leftJoin),
            InnerJoinExpression innerJoin => VisitInnerJoin(innerJoin),
            CrossJoinExpression crossJoin => VisitCrossJoin(crossJoin),
            CrossApplyExpression crossApply => VisitCrossApply(crossApply),
            OuterApplyExpression outerApply => VisitOuterApply(outerApply),
            SelectExpression nestedSelect => (TableExpressionBase)VisitSelect(nestedSelect),
            _ => table
        };
    }

    private TableExpressionBase VisitCrossApply(CrossApplyExpression crossApply)
    {
        DebugLog($"VisitCrossApply: inner table type = {crossApply.Table.GetType().Name}");

        // Save current outer aliases - these become ancestors for the nested select
        var ancestorsForNested = new HashSet<string>(_outerTableAliases);

        var newTable = VisitTableExpression(crossApply.Table);

        // Check if the applied table is a subquery with correlated predicates or projections
        if (newTable is SelectExpression innerSelect)
        {
            DebugLog($"  Inner SELECT has predicate: {innerSelect.Predicate != null}");
            if (innerSelect.Predicate != null)
            {
                DebugLog($"  Contains outer ref in predicate: {ContainsOuterReference(innerSelect.Predicate)}");
            }

            // Check for correlated projections
            var hasCorrelatedProjections = innerSelect.Projection.Any(p => ContainsOuterReference(p.Expression));
            DebugLog($"  Has correlated projections: {hasCorrelatedProjections}");

            if (ContainsCorrelatedPredicates(innerSelect) || hasCorrelatedProjections)
            {
                var savedAncestorAliases = _ancestorAliases;
                _ancestorAliases = ancestorsForNested;

                var (result, correlatedProjectionRemappings) = ExtractCorrelatedPredicatesFromSelectWithProjections(innerSelect);

                _ancestorAliases = savedAncestorAliases;

                if (result != null && result.JoinPredicate != null)
                {
                    // Store remappings for the outer SELECT to use
                    foreach (var kvp in correlatedProjectionRemappings)
                    {
                        _correlatedProjectionRemappings[kvp.Key] = kvp.Value;
                        DebugLog($"  Stored remapping: {kvp.Key} -> {DebugPrintExpression(kvp.Value)}");
                    }

                    DebugLog($"  Successfully extracted, returning InnerJoinExpression");
                    return new InnerJoinExpression(result.TransformedSelect, result.JoinPredicate);
                }
                else
                {
                    DebugLog($"  Failed to extract correlations");
                }
            }
        }

        if (newTable != crossApply.Table)
        {
            return new CrossApplyExpression(newTable);
        }

        return crossApply;
    }

    private TableExpressionBase VisitOuterApply(OuterApplyExpression outerApply)
    {
        DebugLog($"VisitOuterApply: inner table type = {outerApply.Table.GetType().Name}");

        var ancestorsForNested = new HashSet<string>(_outerTableAliases);

        var newTable = VisitTableExpression(outerApply.Table);

        if (newTable is SelectExpression innerSelect)
        {
            var hasCorrelatedProjections = innerSelect.Projection.Any(p => ContainsOuterReference(p.Expression));

            if (ContainsCorrelatedPredicates(innerSelect) || hasCorrelatedProjections)
            {
                var savedAncestorAliases = _ancestorAliases;
                _ancestorAliases = ancestorsForNested;

                var (result, correlatedProjectionRemappings) = ExtractCorrelatedPredicatesFromSelectWithProjections(innerSelect);

                _ancestorAliases = savedAncestorAliases;

                if (result != null && result.JoinPredicate != null)
                {
                    // Store remappings for the outer SELECT to use
                    foreach (var kvp in correlatedProjectionRemappings)
                    {
                        _correlatedProjectionRemappings[kvp.Key] = kvp.Value;
                        DebugLog($"  Stored remapping: {kvp.Key} -> {DebugPrintExpression(kvp.Value)}");
                    }

                    DebugLog($"  Successfully extracted, returning LeftJoinExpression");
                    return new LeftJoinExpression(result.TransformedSelect, result.JoinPredicate);
                }
            }
        }

        if (newTable != outerApply.Table)
        {
            return new OuterApplyExpression(newTable);
        }

        return outerApply;
    }

    /// <summary>
    /// Extracts correlated predicates from a SELECT, also handling correlated projections.
    /// Correlated projections (e.g., SELECT c.ContactName FROM Orders WHERE c.CustomerID = o.CustomerID)
    /// are tracked for remapping - the outer SELECT's references to these columns will be
    /// remapped to reference the outer table directly.
    /// Also handles complex correlated projections (e.g., SELECT c.CustomerID + c.City AS Complex).
    /// </summary>
    private (TransformResult? result, Dictionary<(string, string), SqlExpression> remappings)
        ExtractCorrelatedPredicatesFromSelectWithProjections(SelectExpression innerSelect)
    {
        var remappings = new Dictionary<(string, string), SqlExpression>();

        // Separate projections into correlated and non-correlated
        // A correlated projection is one that references outer columns (either simple or complex)
        var correlatedProjections = new List<(ProjectionExpression projection, SqlExpression outerExpression)>();
        var nonCorrelatedProjections = new List<ProjectionExpression>();

        foreach (var projection in innerSelect.Projection)
        {
            if (projection.Expression is ColumnExpression col && _ancestorAliases.Contains(col.TableAlias))
            {
                // Simple correlated projection: just an outer column reference
                correlatedProjections.Add((projection, col));
                DebugLog($"    Simple correlated projection: {projection.Alias} -> {col.TableAlias}.{col.Name}");
            }
            else if (ContainsOuterReference(projection.Expression))
            {
                // Complex correlated projection: contains outer references (e.g., c.CustomerID + c.City)
                // We can handle this if ALL columns in the expression are from outer tables
                if (ContainsOnlyOuterReferences(projection.Expression))
                {
                    correlatedProjections.Add((projection, projection.Expression));
                    DebugLog($"    Complex correlated projection (outer-only): {projection.Alias} -> {DebugPrintExpression(projection.Expression)}");
                }
                else
                {
                    // Mixed inner and outer references - can't handle
                    DebugLog($"    Mixed inner/outer correlated projection, bailing out: {DebugPrintExpression(projection.Expression)}");
                    return (null, remappings);
                }
            }
            else
            {
                nonCorrelatedProjections.Add(projection);
            }
        }

        // Build remappings for correlated projections
        // When we remove c.ContactName from o0, outer refs to o0.ContactName need to become c.ContactName
        // For complex expressions like c.CustomerID + c.City, o0.Complex becomes the full expression
        foreach (var (projection, outerExpression) in correlatedProjections)
        {
            remappings[(innerSelect.Alias!, projection.Alias)] = outerExpression;
        }

        // Process nested tables to extract their correlated predicates
        var (transformedTables, nestedCorrelations) = ExtractNestedCorrelations(innerSelect.Tables);

        // Process the main predicate to extract correlated parts
        var (correlatedParts, nonCorrelatedParts, complexCorrelatedParts) = innerSelect.Predicate != null
            ? SplitPredicate(innerSelect.Predicate)
            : (new List<CorrelationInfo>(), new List<SqlExpression>(), new List<SqlExpression>());

        // Combine all correlations, but filter out "both-sides-outer" correlations
        // where the "inner" expression is actually an ancestor column
        var allCorrelations = correlatedParts.ToList();
        var bothSidesOuterPredicates = new List<SqlExpression>();
        DebugLog($"    nestedCorrelations count: {nestedCorrelations.Count}, _ancestorAliases: [{string.Join(",", _ancestorAliases)}]");
        foreach (var (outerCol, innerExpr) in nestedCorrelations)
        {
            DebugLog($"      Checking correlation: outer={outerCol.TableAlias}.{outerCol.Name}, inner={DebugPrintExpression(innerExpr)}");
            // Check if the "inner" expression is actually an ancestor column (both sides are outer)
            if (innerExpr is ColumnExpression innerCol && _ancestorAliases.Contains(innerCol.TableAlias))
            {
                // Both sides are ancestor columns - this predicate should be passed to the outer
                // JOIN ON clause directly without projection. We'll handle it separately.
                DebugLog($"    Both-sides-outer nested correlation: {outerCol.TableAlias}.{outerCol.Name} = {innerCol.TableAlias}.{innerCol.Name}");
                bothSidesOuterPredicates.Add(_sqlExpressionFactory.Equal(outerCol, innerCol));
            }
            else
            {
                allCorrelations.Add(new CorrelationInfo(outerCol, innerExpr,
                    (SqlBinaryExpression)_sqlExpressionFactory.Equal(outerCol, innerExpr)));
            }
        }

        // Check for correlated orderings BEFORE deciding what to do
        // Even if no predicate/projection correlations, we might have ordering correlations that need to be removed
        var hasCorrelatedOrderings = innerSelect.Orderings.Any(o => ContainsOuterReference(o.Expression));
        var hasMixedOrderings = innerSelect.Orderings.Any(o =>
            ContainsOuterReference(o.Expression) && !ContainsOnlyOuterReferences(o.Expression));

        if (hasMixedOrderings)
        {
            // Mixed ordering (references both inner and outer) - cannot handle
            DebugLog($"    Cannot handle mixed outer/inner ordering in orderings");
            return (null, remappings);
        }

        // Process orderings - remove outer-only orderings (they're constant in JOIN context)
        var newOrderings = new List<OrderingExpression>();
        foreach (var ordering in innerSelect.Orderings)
        {
            if (ContainsOuterReference(ordering.Expression))
            {
                // Outer-only ordering - remove it
                DebugLog($"    Removing outer-only ordering: {DebugPrintExpression(ordering.Expression)}");
            }
            else
            {
                newOrderings.Add(ordering);
            }
        }

        DebugLog($"    Correlation count: {allCorrelations.Count}, complex: {complexCorrelatedParts.Count}, correlated orderings: {hasCorrelatedOrderings}, bothSidesOuter: {bothSidesOuterPredicates.Count}");

        if (allCorrelations.Count == 0 && complexCorrelatedParts.Count == 0)
        {
            // No predicate correlations that need projection, but might have:
            // - correlated projections
            // - correlated orderings
            // - both-sides-outer predicates (like u.FullName = u.Nickname where both are from outer)
            var needsTransform = (correlatedProjections.Count > 0 && nonCorrelatedProjections.Count > 0)
                || hasCorrelatedOrderings
                || bothSidesOuterPredicates.Count > 0;

            if (needsTransform)
            {
                // Create a simple JOIN with appropriate predicate
                var projectionsToUse = correlatedProjections.Count > 0 ? nonCorrelatedProjections : innerSelect.Projection.ToList();

                // If we removed all projections due to correlations, add a dummy one
                if (projectionsToUse.Count == 0)
                {
                    projectionsToUse = [new ProjectionExpression(
                        _sqlExpressionFactory.Constant(1, _typeMappingSource.FindMapping(typeof(int))),
                        "_dummy")];
                }

                var newInner = innerSelect.Update(
                    transformedTables ?? innerSelect.Tables,
                    innerSelect.Predicate,
                    innerSelect.GroupBy,
                    innerSelect.Having,
                    projectionsToUse,
                    newOrderings,
                    innerSelect.Offset,
                    innerSelect.Limit);

                // Build join predicate from both-sides-outer predicates
                SqlExpression? joinPred = null;
                foreach (var pred in bothSidesOuterPredicates)
                {
                    joinPred = joinPred == null ? pred : _sqlExpressionFactory.AndAlso(joinPred, pred);
                }

                // For orderings-only or projection-only correlations without both-sides-outer,
                // return with null join predicate (existing JOIN predicate is sufficient)
                // For projection correlations without both-sides-outer, return with TRUE predicate
                if (joinPred == null && correlatedProjections.Count > 0)
                {
                    joinPred = _sqlExpressionFactory.Constant(true, _typeMappingSource.FindMapping(typeof(bool)));
                }

                DebugLog($"    Transform successful (no-projection path), join predicate: {(joinPred != null ? DebugPrintExpression(joinPred) : "null")}");
                return (new TransformResult(newInner, joinPred), remappings);
            }
            return (null, remappings);
        }

        // Build the join predicate from correlations
        SqlExpression? joinPredicate = null;
        var additionalProjections = new List<ProjectionExpression>();
        var projectionMapping = new Dictionary<ColumnExpression, ColumnExpression>();

        foreach (var correlation in allCorrelations)
        {
            if (correlation.InnerExpr is ColumnExpression innerCol)
            {
                EnsureColumnProjected(innerSelect, innerCol, additionalProjections, projectionMapping);

                if (projectionMapping.TryGetValue(innerCol, out var projectedCol))
                {
                    var joinCondition = CreateComparisonWithSameOperator(
                        correlation.OriginalPredicate, correlation.OuterCol, projectedCol);
                    joinPredicate = joinPredicate == null ? joinCondition : _sqlExpressionFactory.AndAlso(joinPredicate, joinCondition);
                }
                else
                {
                    var remappedCol = RemapColumnToSelectAlias(innerCol, innerSelect);
                    var joinCondition = CreateComparisonWithSameOperator(
                        correlation.OriginalPredicate, correlation.OuterCol, remappedCol);
                    joinPredicate = joinPredicate == null ? joinCondition : _sqlExpressionFactory.AndAlso(joinPredicate, joinCondition);
                }
            }
            else
            {
                var innerColumns = FindInnerColumns(correlation.OriginalPredicate);
                foreach (var col in innerColumns)
                {
                    EnsureColumnProjected(innerSelect, col, additionalProjections, projectionMapping);
                }
                var remappedPred = RemapPredicateColumns(correlation.OriginalPredicate, innerSelect, projectionMapping);
                joinPredicate = joinPredicate == null ? remappedPred : _sqlExpressionFactory.AndAlso(joinPredicate, remappedPred);
            }
        }

        // Add complex correlated predicates (remapped)
        foreach (var complexPred in complexCorrelatedParts)
        {
            var innerColumns = FindInnerColumns(complexPred);
            if (innerColumns.Count == 0)
                continue;
            foreach (var innerCol in innerColumns)
            {
                EnsureColumnProjected(innerSelect, innerCol, additionalProjections, projectionMapping);
            }

            var remappedPred = RemapPredicateColumns(complexPred, innerSelect, projectionMapping);
            joinPredicate = joinPredicate == null ? remappedPred : _sqlExpressionFactory.AndAlso(joinPredicate, remappedPred);
        }

        // Add both-sides-outer predicates directly to the join predicate (no projection needed)
        foreach (var bothSidesPred in bothSidesOuterPredicates)
        {
            joinPredicate = joinPredicate == null ? bothSidesPred : _sqlExpressionFactory.AndAlso(joinPredicate, bothSidesPred);
        }

        // Build the new inner SELECT without correlated predicates and without correlated projections
        SqlExpression? newPredicate = null;
        foreach (var pred in nonCorrelatedParts)
        {
            newPredicate = newPredicate == null ? pred : _sqlExpressionFactory.AndAlso(newPredicate, pred);
        }

        // Use non-correlated projections plus additional projections for correlations
        var newProjections = nonCorrelatedProjections.ToList();
        newProjections.AddRange(additionalProjections);

        // If we removed all projections, add a dummy one
        if (newProjections.Count == 0)
        {
            newProjections.Add(new ProjectionExpression(
                _sqlExpressionFactory.Constant(1, _typeMappingSource.FindMapping(typeof(int))),
                "_dummy"));
        }

        // If this SELECT has GROUP BY and we added projections, we need to add them to GROUP BY too
        var newGroupBy = innerSelect.GroupBy.ToList();
        if (innerSelect.GroupBy.Count > 0 && additionalProjections.Count > 0)
        {
            DebugLog($"    Adding {additionalProjections.Count} correlation projections to GROUP BY");
            foreach (var addedProj in additionalProjections)
            {
                // Add the column to GROUP BY (the actual expression, not the projection)
                if (addedProj.Expression is ColumnExpression col)
                {
                    DebugLog($"      Adding to GROUP BY: {col.TableAlias}.{col.Name}");
                    newGroupBy.Add(col);
                }
            }
        }

        // Note: Correlated orderings were already handled at the start of this method
        // (outer-only orderings removed, mixed orderings cause early return)

        var tablesToUse = transformedTables ?? innerSelect.Tables;

        var transformedSelect = innerSelect.Update(
            tablesToUse,
            newPredicate,
            newGroupBy,
            innerSelect.Having,
            newProjections,
            newOrderings,
            innerSelect.Offset,
            innerSelect.Limit);

        DebugLog($"    Transform successful, join predicate created");

        return (new TransformResult(transformedSelect, joinPredicate), remappings);
    }

    private TableExpressionBase VisitLeftJoin(LeftJoinExpression leftJoin)
    {
        DebugLog($"VisitLeftJoin: table={leftJoin.Table.GetType().Name}, alias={leftJoin.Table.Alias}");
        DebugLog($"  Outer aliases: [{string.Join(",", _outerTableAliases)}]");

        // Save current outer aliases - these become ancestors for the nested select
        var ancestorsForNested = new HashSet<string>(_outerTableAliases);

        var newTable = VisitTableExpression(leftJoin.Table);

        // Check if the joined table is a subquery with correlated predicates or projections
        if (newTable is SelectExpression innerSelect)
        {
            var hasCorrelatedPredicates = ContainsCorrelatedPredicates(innerSelect);
            var hasCorrelatedProjections = innerSelect.Projection.Any(p => ContainsOuterReference(p.Expression));
            DebugLog($"  Inner SELECT alias={innerSelect.Alias}, has correlated predicates={hasCorrelatedPredicates}, has correlated projections={hasCorrelatedProjections}");

            if (hasCorrelatedPredicates || hasCorrelatedProjections)
            {
                // Set up ancestor context for extraction - the outer tables are ancestors from the nested select's perspective
                var savedAncestorAliases = _ancestorAliases;
                _ancestorAliases = ancestorsForNested;

                DebugLog($"  Calling ExtractCorrelatedPredicatesFromSelectWithProjections, ancestors=[{string.Join(",", _ancestorAliases)}]");
                var (result, correlatedProjectionRemappings) = ExtractCorrelatedPredicatesFromSelectWithProjections(innerSelect);

                _ancestorAliases = savedAncestorAliases;

                DebugLog($"  ExtractCorrelatedPredicatesFromSelectWithProjections result: {(result != null ? "success" : "null")}");

                if (result != null)
                {
                    // Store remappings for the outer SELECT to use
                    foreach (var kvp in correlatedProjectionRemappings)
                    {
                        _correlatedProjectionRemappings[kvp.Key] = kvp.Value;
                        DebugLog($"  Stored remapping: {kvp.Key} -> {DebugPrintExpression(kvp.Value)}");
                    }

                    var combinedPredicate = result.JoinPredicate != null
                        ? _sqlExpressionFactory.AndAlso(leftJoin.JoinPredicate, result.JoinPredicate)
                        : leftJoin.JoinPredicate;
                    DebugLog($"  Returning transformed LeftJoinExpression");
                    return new LeftJoinExpression(result.TransformedSelect, combinedPredicate, leftJoin.IsPrunable);
                }
            }
        }

        if (newTable != leftJoin.Table)
        {
            return new LeftJoinExpression(newTable, leftJoin.JoinPredicate, leftJoin.IsPrunable);
        }

        return leftJoin;
    }

    private TableExpressionBase VisitInnerJoin(InnerJoinExpression innerJoin)
    {
        DebugLog($"VisitInnerJoin: table={innerJoin.Table.GetType().Name}, alias={innerJoin.Table.Alias}");
        DebugLog($"  Outer aliases: [{string.Join(",", _outerTableAliases)}]");

        // Save current outer aliases - these become ancestors for the nested select
        var ancestorsForNested = new HashSet<string>(_outerTableAliases);

        var newTable = VisitTableExpression(innerJoin.Table);

        // Check if the joined table is a subquery with correlated predicates or projections
        if (newTable is SelectExpression innerSelect)
        {
            var hasCorrelatedPredicates = ContainsCorrelatedPredicates(innerSelect);
            var hasCorrelatedProjections = innerSelect.Projection.Any(p => ContainsOuterReference(p.Expression));
            DebugLog($"  Inner SELECT alias={innerSelect.Alias}, has correlated predicates={hasCorrelatedPredicates}, has correlated projections={hasCorrelatedProjections}");

            if (hasCorrelatedPredicates || hasCorrelatedProjections)
            {
                // Set up ancestor context for extraction - the outer tables are ancestors from the nested select's perspective
                var savedAncestorAliases = _ancestorAliases;
                _ancestorAliases = ancestorsForNested;

                DebugLog($"  Calling ExtractCorrelatedPredicatesFromSelectWithProjections, ancestors=[{string.Join(",", _ancestorAliases)}]");
                var (result, correlatedProjectionRemappings) = ExtractCorrelatedPredicatesFromSelectWithProjections(innerSelect);

                _ancestorAliases = savedAncestorAliases;

                DebugLog($"  ExtractCorrelatedPredicatesFromSelectWithProjections result: {(result != null ? "success" : "null")}");

                if (result != null)
                {
                    // Store remappings for the outer SELECT to use
                    foreach (var kvp in correlatedProjectionRemappings)
                    {
                        _correlatedProjectionRemappings[kvp.Key] = kvp.Value;
                        DebugLog($"  Stored remapping: {kvp.Key} -> {DebugPrintExpression(kvp.Value)}");
                    }

                    var combinedPredicate = result.JoinPredicate != null
                        ? _sqlExpressionFactory.AndAlso(innerJoin.JoinPredicate, result.JoinPredicate)
                        : innerJoin.JoinPredicate;
                    DebugLog($"  Returning transformed InnerJoinExpression");
                    return new InnerJoinExpression(result.TransformedSelect, combinedPredicate, innerJoin.IsPrunable);
                }
            }
        }

        if (newTable != innerJoin.Table)
        {
            return new InnerJoinExpression(newTable, innerJoin.JoinPredicate, innerJoin.IsPrunable);
        }

        return innerJoin;
    }

    private TableExpressionBase VisitCrossJoin(CrossJoinExpression crossJoin)
    {
        // Save current outer aliases - these become ancestors for the nested select
        var ancestorsForNested = new HashSet<string>(_outerTableAliases);

        var newTable = VisitTableExpression(crossJoin.Table);

        // Check if the joined table is a subquery with correlated predicates
        // For CROSS JOIN, we need to convert to INNER JOIN and use the correlation as the ON clause
        if (newTable is SelectExpression innerSelect && ContainsCorrelatedPredicates(innerSelect))
        {
            // Set up ancestor context for extraction
            var savedAncestorAliases = _ancestorAliases;
            _ancestorAliases = ancestorsForNested;

            var result = ExtractCorrelatedPredicatesFromSelect(innerSelect);

            _ancestorAliases = savedAncestorAliases;

            if (result != null && result.JoinPredicate != null)
            {
                // Convert CROSS JOIN to INNER JOIN with the extracted predicate
                return new InnerJoinExpression(result.TransformedSelect, result.JoinPredicate);
            }
        }

        if (newTable != crossJoin.Table)
        {
            return new CrossJoinExpression(newTable);
        }

        return crossJoin;
    }

    /// <summary>
    /// Checks if a SelectExpression or any of its nested tables contain expressions
    /// that reference outer table aliases (correlated predicates, projections, or orderings).
    /// </summary>
    private bool ContainsCorrelatedPredicates(SelectExpression select)
    {
        DebugLog($"  ContainsCorrelatedPredicates: SELECT alias={select.Alias}, OuterAliases=[{string.Join(",", _outerTableAliases)}]");
        DebugLog($"    Tables: {string.Join(", ", select.Tables.Select(t => t.GetType().Name + ":" + t.UnwrapJoin().Alias))}");

        // Check the main predicate
        if (select.Predicate != null && ContainsOuterReference(select.Predicate))
            return true;

        // Check projections
        foreach (var projection in select.Projection)
        {
            if (ContainsOuterReference(projection.Expression))
                return true;
        }

        // Check orderings - important for correlated ORDER BY clauses
        foreach (var ordering in select.Orderings)
        {
            if (ContainsOuterReference(ordering.Expression))
                return true;
        }

        // Check nested tables recursively
        foreach (var table in select.Tables)
        {
            if (ContainsCorrelatedPredicatesInTable(table))
                return true;
        }

        return false;
    }

    private bool ContainsCorrelatedPredicatesInTable(TableExpressionBase table)
    {
        switch (table)
        {
            case SelectExpression nested:
                return ContainsCorrelatedPredicates(nested);

            case PredicateJoinExpressionBase predicateJoin:
                var hasOuterRef = ContainsOuterReference(predicateJoin.JoinPredicate);
                if (hasOuterRef)
                {
                    DebugLog($"    Found correlated JOIN predicate in {predicateJoin.GetType().Name}, alias={predicateJoin.Table.Alias}");
                    DebugLog($"      Predicate: {DebugPrintExpression(predicateJoin.JoinPredicate)}");
                    DebugLog($"      OuterAliases: [{string.Join(",", _outerTableAliases)}]");
                    return true;
                }
                return ContainsCorrelatedPredicatesInTable(predicateJoin.Table);

            case CrossJoinExpression crossJoin:
                return ContainsCorrelatedPredicatesInTable(crossJoin.Table);

            default:
                return false;
        }
    }

    private bool ContainsOuterReference(SqlExpression expression)
    {
        var finder = new OuterColumnFinder(_outerTableAliases);
        finder.Visit(expression);
        return finder.OuterColumns.Count > 0;
    }

    /// <summary>
    /// Checks if ALL column references in an expression are from outer tables.
    /// This is used to determine if a complex projection can be safely remapped.
    /// Returns true if the expression contains ONLY outer column references (no inner references).
    /// </summary>
    private bool ContainsOnlyOuterReferences(SqlExpression expression)
    {
        var innerFinder = new InnerColumnFinder(_outerTableAliases);
        innerFinder.Visit(expression);
        // If there are no inner columns, all columns are outer
        return innerFinder.InnerColumns.Count == 0;
    }

    /// <summary>
    /// Extracts correlated predicates from a SelectExpression and returns the transformed
    /// SelectExpression with the extracted join predicate.
    /// </summary>
    private TransformResult? ExtractCorrelatedPredicatesFromSelect(SelectExpression innerSelect)
    {
        DebugLog($"  ExtractCorrelatedPredicatesFromSelect: SELECT alias={innerSelect.Alias}");
        DebugLog($"    Tables: {string.Join(", ", innerSelect.Tables.Select(t => t.GetType().Name + ":" + t.Alias))}");
        DebugLog($"    Predicate: {innerSelect.Predicate?.ToString() ?? "null"}");
        DebugLog($"    GroupBy count: {innerSelect.GroupBy.Count}");

        // Process nested tables to extract their correlated predicates
        var (transformedTables, nestedCorrelations) = ExtractNestedCorrelations(innerSelect.Tables);
        DebugLog($"    Nested correlations extracted: {nestedCorrelations.Count}");

        // Process the main predicate to extract correlated parts
        var (correlatedParts, nonCorrelatedParts, complexCorrelatedParts) = innerSelect.Predicate != null
            ? SplitPredicate(innerSelect.Predicate)
            : (new List<CorrelationInfo>(), new List<SqlExpression>(), new List<SqlExpression>());

        // Combine all correlations (convert nested to CorrelationInfo format)
        // but filter out "both-sides-outer" correlations where the "inner" is actually an ancestor
        var allCorrelations = correlatedParts.ToList();
        var bothSidesOuterPredicates = new List<SqlExpression>();
        foreach (var (outerCol, innerExpr) in nestedCorrelations)
        {
            // Check if the "inner" expression is actually an ancestor column (both sides are outer)
            if (innerExpr is ColumnExpression innerCol && _ancestorAliases.Contains(innerCol.TableAlias))
            {
                // Both sides are ancestor columns - pass directly to join predicate without projection
                DebugLog($"    Both-sides-outer nested correlation: {outerCol.TableAlias}.{outerCol.Name} = {innerCol.TableAlias}.{innerCol.Name}");
                bothSidesOuterPredicates.Add(_sqlExpressionFactory.Equal(outerCol, innerCol));
            }
            else
            {
                // For nested correlations, we don't have the original predicate, so create an Equal
                allCorrelations.Add(new CorrelationInfo(outerCol, innerExpr,
                    (SqlBinaryExpression)_sqlExpressionFactory.Equal(outerCol, innerExpr)));
            }
        }

        // Check for correlated orderings BEFORE returning
        // Even if no predicate correlations, we might have ordering correlations
        var hasCorrelatedOrderings = innerSelect.Orderings.Any(o => ContainsOuterReference(o.Expression));
        var hasMixedOrderings = innerSelect.Orderings.Any(o =>
            ContainsOuterReference(o.Expression) && !ContainsOnlyOuterReferences(o.Expression));

        if (hasMixedOrderings)
        {
            // Mixed ordering - cannot handle
            DebugLog($"    Cannot handle mixed outer/inner ordering in orderings");
            return null;
        }

        if (allCorrelations.Count == 0 && complexCorrelatedParts.Count == 0
            && !hasCorrelatedOrderings && bothSidesOuterPredicates.Count == 0)
        {
            return null;
        }

        // Build the join predicate from correlations
        SqlExpression? joinPredicate = null;
        var additionalProjections = new List<ProjectionExpression>();
        var projectionMapping = new Dictionary<ColumnExpression, ColumnExpression>();

        // For each correlation, ensure the inner column is projected and add to join predicate
        foreach (var correlation in allCorrelations)
        {
            if (correlation.InnerExpr is ColumnExpression innerCol)
            {
                EnsureColumnProjected(innerSelect, innerCol, additionalProjections, projectionMapping);

                if (projectionMapping.TryGetValue(innerCol, out var projectedCol))
                {
                    // Use the same operator as the original predicate
                    var joinCondition = CreateComparisonWithSameOperator(
                        correlation.OriginalPredicate, correlation.OuterCol, projectedCol);
                    joinPredicate = joinPredicate == null ? joinCondition : _sqlExpressionFactory.AndAlso(joinPredicate, joinCondition);
                }
                else
                {
                    // Column should have been projected, use direct reference
                    var remappedCol = RemapColumnToSelectAlias(innerCol, innerSelect);
                    var joinCondition = CreateComparisonWithSameOperator(
                        correlation.OriginalPredicate, correlation.OuterCol, remappedCol);
                    joinPredicate = joinPredicate == null ? joinCondition : _sqlExpressionFactory.AndAlso(joinPredicate, joinCondition);
                }
            }
            else
            {
                // Inner expression is not a column (e.g., NULL constant)
                // Remap the entire original predicate
                var innerColumns = FindInnerColumns(correlation.OriginalPredicate);
                foreach (var col in innerColumns)
                {
                    EnsureColumnProjected(innerSelect, col, additionalProjections, projectionMapping);
                }
                var remappedPred = RemapPredicateColumns(correlation.OriginalPredicate, innerSelect, projectionMapping);
                joinPredicate = joinPredicate == null ? remappedPred : _sqlExpressionFactory.AndAlso(joinPredicate, remappedPred);
            }
        }

        // Add complex correlated predicates (remapped)
        // Only add if they have inner columns - predicates with only outer columns shouldn't be here
        foreach (var complexPred in complexCorrelatedParts)
        {
            var innerColumns = FindInnerColumns(complexPred);
            if (innerColumns.Count == 0)
            {
                continue; // Skip predicates that have no inner columns
            }
            foreach (var innerCol in innerColumns)
            {
                EnsureColumnProjected(innerSelect, innerCol, additionalProjections, projectionMapping);
            }

            var remappedPred = RemapPredicateColumns(complexPred, innerSelect, projectionMapping);
            joinPredicate = joinPredicate == null ? remappedPred : _sqlExpressionFactory.AndAlso(joinPredicate, remappedPred);
        }

        // Add both-sides-outer predicates directly to the join predicate (no projection needed)
        foreach (var bothSidesPred in bothSidesOuterPredicates)
        {
            joinPredicate = joinPredicate == null ? bothSidesPred : _sqlExpressionFactory.AndAlso(joinPredicate, bothSidesPred);
        }

        // Build the new inner SELECT without correlated predicates
        SqlExpression? newPredicate = null;
        foreach (var pred in nonCorrelatedParts)
        {
            newPredicate = newPredicate == null ? pred : _sqlExpressionFactory.AndAlso(newPredicate, pred);
        }

        var newProjections = innerSelect.Projection.ToList();
        newProjections.AddRange(additionalProjections);

        // If this SELECT has GROUP BY and we added projections, we need to add them to GROUP BY too
        var newGroupBy = innerSelect.GroupBy.ToList();
        if (innerSelect.GroupBy.Count > 0 && additionalProjections.Count > 0)
        {
            foreach (var addedProj in additionalProjections)
            {
                // Add the column to GROUP BY (the actual expression, not the projection)
                if (addedProj.Expression is ColumnExpression col)
                {
                    newGroupBy.Add(col);
                }
            }
        }

        // Handle correlated orderings
        // - Orderings that reference only outer columns can be removed (they're constant in the JOIN context)
        // - Orderings that mix outer and inner columns cannot be properly handled
        var newOrderings = new List<OrderingExpression>();
        foreach (var ordering in innerSelect.Orderings)
        {
            if (ContainsOuterReference(ordering.Expression))
            {
                if (ContainsOnlyOuterReferences(ordering.Expression))
                {
                    // Pure outer ordering - can be safely removed, it's constant in JOIN context
                    DebugLog($"    Removing outer-only ordering: {DebugPrintExpression(ordering.Expression)}");
                    continue;
                }
                else
                {
                    // Mixed ordering - cannot handle, bail out
                    DebugLog($"    Cannot handle mixed outer/inner ordering: {DebugPrintExpression(ordering.Expression)}");
                    return null;
                }
            }
            else
            {
                newOrderings.Add(ordering);
            }
        }

        var tablesToUse = transformedTables ?? innerSelect.Tables;

        var transformedSelect = innerSelect.Update(
            tablesToUse,
            newPredicate,
            newGroupBy,
            innerSelect.Having,
            newProjections,
            newOrderings,
            innerSelect.Offset,
            innerSelect.Limit);


        return new TransformResult(transformedSelect, joinPredicate);
    }

    /// <summary>
    /// Extracts correlations from nested tables.
    /// </summary>
    private (IReadOnlyList<TableExpressionBase>? transformedTables, List<(ColumnExpression outer, SqlExpression inner)> correlations)
        ExtractNestedCorrelations(IReadOnlyList<TableExpressionBase> tables)
    {
        var correlations = new List<(ColumnExpression outer, SqlExpression inner)>();
        List<TableExpressionBase>? newTables = null;

        for (var i = 0; i < tables.Count; i++)
        {
            var table = tables[i];
            var (newTable, tableCorrelations) = ExtractCorrelationsFromTable(table);

            if (newTable != table)
            {
                newTables ??= tables.Take(i).ToList();
                newTables.Add(newTable);
                correlations.AddRange(tableCorrelations);
            }
            else if (newTables != null)
            {
                newTables.Add(table);
            }
        }

        return (newTables, correlations);
    }

    private (TableExpressionBase table, List<(ColumnExpression outer, SqlExpression inner)> correlations)
        ExtractCorrelationsFromTable(TableExpressionBase table)
    {
        var correlations = new List<(ColumnExpression outer, SqlExpression inner)>();

        switch (table)
        {
            case InnerJoinExpression innerJoin:
                // Check if the join predicate contains ANCESTOR references (not just sibling refs)
                if (ContainsAncestorReference(innerJoin.JoinPredicate))
                {
                    return ExtractCorrelationsFromJoinPredicate(innerJoin, isLeftJoin: false);
                }
                // Check if the table is a SELECT with correlations
                if (innerJoin.Table is SelectExpression innerNestedSelect)
                {
                    return ExtractCorrelationsFromJoinedSelect(innerJoin, innerNestedSelect, isLeftJoin: false);
                }
                break;

            case LeftJoinExpression leftJoin:
                // Check if the join predicate contains ANCESTOR references (not just sibling refs)
                if (ContainsAncestorReference(leftJoin.JoinPredicate))
                {
                    return ExtractCorrelationsFromJoinPredicate(leftJoin, isLeftJoin: true);
                }
                // Check if the table is a SELECT with correlations
                if (leftJoin.Table is SelectExpression leftNestedSelect)
                {
                    return ExtractCorrelationsFromJoinedSelect(leftJoin, leftNestedSelect, isLeftJoin: true);
                }
                break;

            case SelectExpression nestedSelect:
                // Check if this SELECT or any of its nested tables have correlations
                DebugLog($"      ExtractCorrelationsFromTable: SelectExpression alias={nestedSelect.Alias}");
                var hasCorr = ContainsCorrelatedPredicates(nestedSelect);
                DebugLog($"        ContainsCorrelatedPredicates: {hasCorr}");
                if (hasCorr)
                {
                    DebugLog($"        Calling ExtractCorrelationsFromNestedSelectDeep");
                    var result = ExtractCorrelationsFromNestedSelectDeep(nestedSelect);
                    DebugLog($"        Result: {(result != null ? $"{result.Value.correlations.Count} correlations, {result.Value.directPredicates.Count} direct" : "null")}");
                    if (result != null)
                    {
                        // TODO: Handle directPredicates - for now, combine with correlations by converting direct predicates
                        // to correlation entries with the outer column reference
                        var allCorrelations = result.Value.correlations.ToList();
                        // Direct predicates need to be passed up somehow - for now we skip them and let the correlation work
                        // This is a simplification; a proper fix would thread directPredicates through the entire chain
                        return (result.Value.transformedSelect, allCorrelations);
                    }
                }
                break;
        }

        return (table, correlations);
    }

    /// <summary>
    /// Extracts correlations from a JOIN's predicate (ON clause).
    /// Only extracts correlations to ANCESTOR tables (not sibling tables at the same level).
    /// Returns the transformed join with ancestor correlations removed from the predicate.
    /// </summary>
    private (TableExpressionBase table, List<(ColumnExpression outer, SqlExpression inner)> correlations)
        ExtractCorrelationsFromJoinPredicate(PredicateJoinExpressionBase join, bool isLeftJoin)
    {
        var correlations = new List<(ColumnExpression outer, SqlExpression inner)>();

        // Split the join predicate using ancestor aliases only
        // Correlations to sibling tables should stay in the join predicate
        var (ancestorCorrelations, remainingPredicates, complexAncestorCorrelations) =
            SplitPredicateForAncestors(join.JoinPredicate);


        // If we have complex ancestor correlations, we can't handle them
        if (complexAncestorCorrelations.Count > 0)
        {
            return (join, correlations);
        }

        // If no ancestor correlations to extract, return unchanged
        if (ancestorCorrelations.Count == 0)
        {
            return (join, correlations);
        }

        // For ancestor correlations, we need to project the inner columns through the joined table
        var joinedTable = join.Table;
        var additionalProjections = new List<ProjectionExpression>();
        var projectionMapping = new Dictionary<ColumnExpression, ColumnExpression>();

        // If the joined table is a SelectExpression, we can add projections to it
        if (joinedTable is SelectExpression joinedSelect)
        {
            foreach (var correlation in ancestorCorrelations)
            {
                if (correlation.InnerExpr == null)
                {
                    // Both sides are outer columns - the OriginalPredicate can be lifted directly
                    // Extract both columns from the original predicate and add as correlation
                    if (correlation.OriginalPredicate is SqlBinaryExpression binExpr &&
                        binExpr.Left is ColumnExpression leftCol &&
                        binExpr.Right is ColumnExpression rightCol)
                    {
                        // Add as (left, right) - will become left = right in outer ON clause
                        correlations.Add((leftCol, rightCol));
                    }
                }
                else if (correlation.InnerExpr is ColumnExpression innerCol)
                {
                    EnsureColumnProjected(joinedSelect, innerCol, additionalProjections, projectionMapping);
                    if (projectionMapping.TryGetValue(innerCol, out var projectedCol))
                    {
                        correlations.Add((correlation.OuterCol, projectedCol));
                    }
                }
            }

            // Build new predicate from remaining (non-ancestor) predicates
            SqlExpression? newJoinPredicate = null;
            foreach (var pred in remainingPredicates)
            {
                newJoinPredicate = newJoinPredicate == null ? pred : _sqlExpressionFactory.AndAlso(newJoinPredicate, pred);
            }

            // If no predicates remain, use TRUE
            newJoinPredicate ??= _sqlExpressionFactory.Constant(true, _typeMappingSource.FindMapping(typeof(bool)));

            // Update the select with additional projections
            var newProjections = joinedSelect.Projection.ToList();
            newProjections.AddRange(additionalProjections);

            var transformedSelect = joinedSelect.Update(
                joinedSelect.Tables,
                joinedSelect.Predicate,
                joinedSelect.GroupBy,
                joinedSelect.Having,
                newProjections,
                joinedSelect.Orderings,
                joinedSelect.Offset,
                joinedSelect.Limit);

            var newJoin = isLeftJoin
                ? (TableExpressionBase)new LeftJoinExpression(transformedSelect, newJoinPredicate, join.IsPrunable)
                : new InnerJoinExpression(transformedSelect, newJoinPredicate, join.IsPrunable);

            return (newJoin, correlations);
        }

        // If joined table is not a SelectExpression, we can't extract correlations this way
        return (join, correlations);
    }

    /// <summary>
    /// Splits a predicate into:
    /// 1. Correlations to ANCESTOR tables (need to be lifted)
    /// 2. Remaining predicates (non-correlated + correlations to siblings)
    /// 3. Complex ancestor correlations (can't handle)
    /// </summary>
    private (List<CorrelationInfo> ancestorCorrelations,
             List<SqlExpression> remaining,
             List<SqlExpression> complexAncestorCorrelations) SplitPredicateForAncestors(SqlExpression predicate)
    {
        var ancestorCorrelations = new List<CorrelationInfo>();
        var remaining = new List<SqlExpression>();
        var complexAncestorCorrelations = new List<SqlExpression>();

        SplitPredicateForAncestorsRecursive(predicate, ancestorCorrelations, remaining, complexAncestorCorrelations);

        return (ancestorCorrelations, remaining, complexAncestorCorrelations);
    }

    private void SplitPredicateForAncestorsRecursive(
        SqlExpression predicate,
        List<CorrelationInfo> ancestorCorrelations,
        List<SqlExpression> remaining,
        List<SqlExpression> complexAncestorCorrelations)
    {
        if (predicate is SqlBinaryExpression { OperatorType: ExpressionType.AndAlso } andExpr)
        {
            SplitPredicateForAncestorsRecursive(andExpr.Left, ancestorCorrelations, remaining, complexAncestorCorrelations);
            SplitPredicateForAncestorsRecursive(andExpr.Right, ancestorCorrelations, remaining, complexAncestorCorrelations);
            return;
        }

        // Check if predicate references ANCESTOR tables (not just any outer table)
        if (!ContainsAncestorReference(predicate))
        {
            // No ancestor reference - this is either non-correlated or a sibling correlation
            // Either way, it stays in the predicate
            remaining.Add(predicate);
            return;
        }

        // Has ancestor references - check if it's a simple comparison we can extract
        // Handle Equal, NotEqual, and other comparison operators (same as SplitPredicateRecursive)
        if (predicate is SqlBinaryExpression binExpr &&
            (binExpr.OperatorType == ExpressionType.Equal ||
             binExpr.OperatorType == ExpressionType.NotEqual ||
             binExpr.OperatorType == ExpressionType.LessThan ||
             binExpr.OperatorType == ExpressionType.LessThanOrEqual ||
             binExpr.OperatorType == ExpressionType.GreaterThan ||
             binExpr.OperatorType == ExpressionType.GreaterThanOrEqual))
        {

            // Check if one side is an ancestor column and the other has no ancestor references
            if (binExpr.Left is ColumnExpression leftCol && _ancestorAliases.Contains(leftCol.TableAlias))
            {
                if (!ContainsAncestorReference(binExpr.Right))
                {
                    ancestorCorrelations.Add(new CorrelationInfo(leftCol, binExpr.Right, binExpr));
                    return;
                }
            }

            if (binExpr.Right is ColumnExpression rightCol && _ancestorAliases.Contains(rightCol.TableAlias))
            {
                if (!ContainsAncestorReference(binExpr.Left))
                {
                    ancestorCorrelations.Add(new CorrelationInfo(rightCol, binExpr.Left, binExpr));
                    return;
                }
            }

            // Special case: BOTH sides are ancestor columns (e.g., u.FullName = u.Nickname)
            // This is a pure outer predicate that can be lifted directly without projection mapping
            // We add it as-is using a null InnerExpr to signal this special case
            if (binExpr.Left is ColumnExpression leftAncestor && _ancestorAliases.Contains(leftAncestor.TableAlias) &&
                binExpr.Right is ColumnExpression rightAncestor && _ancestorAliases.Contains(rightAncestor.TableAlias))
            {
                // Use the original predicate as-is - it will be lifted to the outer ON clause
                // InnerExpr is null to indicate no projection mapping needed
                ancestorCorrelations.Add(new CorrelationInfo(leftAncestor, null!, binExpr));
                return;
            }
        }
        else
        {
        }

        // Complex ancestor correlation - can't handle
        complexAncestorCorrelations.Add(predicate);
    }

    private bool ContainsAncestorReference(SqlExpression expression)
    {
        var finder = new OuterColumnFinder(_ancestorAliases);
        finder.Visit(expression);
        return finder.OuterColumns.Count > 0;
    }

    private (TableExpressionBase table, List<(ColumnExpression outer, SqlExpression inner)> correlations)
        ExtractCorrelationsFromJoinedSelect(PredicateJoinExpressionBase join, SelectExpression nestedSelect, bool isLeftJoin)
    {
        var correlations = new List<(ColumnExpression outer, SqlExpression inner)>();

        // Process nested tables recursively to extract correlations at all levels
        var (transformedNestedTables, nestedCorrelations) = ExtractNestedCorrelations(nestedSelect.Tables);

        // Build lists for tracking additional projections and correlations to lift
        var additionalProjections = new List<ProjectionExpression>();
        var projectionMapping = new Dictionary<ColumnExpression, ColumnExpression>();

        // For each nested correlation, we need to project the inner column through this SELECT
        // so it can be used in the outer JOIN condition
        foreach (var (outerCol, innerCol) in nestedCorrelations)
        {
            if (innerCol is ColumnExpression innerColumn)
            {
                // Check if the "inner" column is actually an ancestor column (both sides are outer).
                // This happens when the original predicate was like u.FullName = u.Nickname
                // where both sides are from the same outer table. In this case, we shouldn't
                // try to project it - just pass the correlation through unchanged.
                if (_ancestorAliases.Contains(innerColumn.TableAlias))
                {
                    DebugLog($"    Both-sides-outer correlation: {outerCol.TableAlias}.{outerCol.Name} = {innerColumn.TableAlias}.{innerColumn.Name}, passing through");
                    correlations.Add((outerCol, innerColumn));
                    continue;
                }

                // Make sure the inner column is projected in this SELECT
                EnsureColumnProjected(nestedSelect, innerColumn, additionalProjections, projectionMapping);

                if (projectionMapping.TryGetValue(innerColumn, out var projectedCol))
                {
                    // The correlation becomes: outerCol = projectedCol (through this SELECT)
                    correlations.Add((outerCol, projectedCol));
                }
            }
        }

        // Also check for correlations in this SELECT's predicate
        if (nestedSelect.Predicate != null && ContainsOuterReference(nestedSelect.Predicate))
        {
            var (correlatedParts, nonCorrelatedParts, complexCorrelated) = SplitPredicate(nestedSelect.Predicate);

            foreach (var correlation in correlatedParts)
            {
                if (correlation.InnerExpr is ColumnExpression innerCol)
                {
                    EnsureColumnProjected(nestedSelect, innerCol, additionalProjections, projectionMapping);
                    if (projectionMapping.TryGetValue(innerCol, out var projectedCol))
                    {
                        correlations.Add((correlation.OuterCol, projectedCol));
                    }
                }
            }

            // Rebuild predicate without correlated parts
            SqlExpression? newPredicate = null;
            foreach (var pred in nonCorrelatedParts)
            {
                newPredicate = newPredicate == null ? pred : _sqlExpressionFactory.AndAlso(newPredicate, pred);
            }

            // Update nested select with new predicate and projections
            var newProjections = nestedSelect.Projection.ToList();
            newProjections.AddRange(additionalProjections);

            var transformedSelect = nestedSelect.Update(
                transformedNestedTables ?? nestedSelect.Tables,
                newPredicate,
                nestedSelect.GroupBy,
                nestedSelect.Having,
                newProjections,
                nestedSelect.Orderings,
                nestedSelect.Offset,
                nestedSelect.Limit);

            var newJoin = isLeftJoin
                ? (TableExpressionBase)new LeftJoinExpression(transformedSelect, join.JoinPredicate, join.IsPrunable)
                : new InnerJoinExpression(transformedSelect, join.JoinPredicate, join.IsPrunable);

            return (newJoin, correlations);
        }

        // If we only have nested correlations (no predicate correlations), update the select and return
        if (transformedNestedTables != null || additionalProjections.Count > 0)
        {
            var newProjections = nestedSelect.Projection.ToList();
            newProjections.AddRange(additionalProjections);

            var transformedSelect = nestedSelect.Update(
                transformedNestedTables ?? nestedSelect.Tables,
                nestedSelect.Predicate,
                nestedSelect.GroupBy,
                nestedSelect.Having,
                newProjections,
                nestedSelect.Orderings,
                nestedSelect.Offset,
                nestedSelect.Limit);

            var newJoin = isLeftJoin
                ? (TableExpressionBase)new LeftJoinExpression(transformedSelect, join.JoinPredicate, join.IsPrunable)
                : new InnerJoinExpression(transformedSelect, join.JoinPredicate, join.IsPrunable);

            return (newJoin, correlations);
        }

        return (join, correlations);
    }

    private (SelectExpression transformedSelect, List<(ColumnExpression outer, SqlExpression inner)> correlations)?
        ExtractCorrelationsFromNestedSelect(SelectExpression nestedSelect)
    {
        if (nestedSelect.Predicate == null)
        {
            return null;
        }

        var (correlatedParts, nonCorrelatedParts, complexCorrelatedParts) = SplitPredicate(nestedSelect.Predicate);

        // For now, only handle simple equality correlations in nested selects
        if (correlatedParts.Count == 0 || complexCorrelatedParts.Count > 0)
        {
            return null;
        }

        // Build the correlation list with columns projected from the nested select
        var correlations = new List<(ColumnExpression outer, SqlExpression inner)>();
        var additionalProjections = new List<ProjectionExpression>();
        var projectionMapping = new Dictionary<ColumnExpression, ColumnExpression>();

        foreach (var correlation in correlatedParts)
        {
            if (correlation.InnerExpr is ColumnExpression innerCol)
            {
                EnsureColumnProjected(nestedSelect, innerCol, additionalProjections, projectionMapping);

                if (projectionMapping.TryGetValue(innerCol, out var projectedCol))
                {
                    correlations.Add((correlation.OuterCol, projectedCol));
                }
                else
                {
                    return null;
                }
            }
            else
            {
                return null;
            }
        }

        // Build the new predicate without correlated parts
        SqlExpression? newPredicate = null;
        foreach (var pred in nonCorrelatedParts)
        {
            newPredicate = newPredicate == null ? pred : _sqlExpressionFactory.AndAlso(newPredicate, pred);
        }

        var newProjections = nestedSelect.Projection.ToList();
        newProjections.AddRange(additionalProjections);

        var transformedSelect = nestedSelect.Update(
            nestedSelect.Tables,
            newPredicate,
            nestedSelect.GroupBy,
            nestedSelect.Having,
            newProjections,
            nestedSelect.Orderings,
            nestedSelect.Offset,
            nestedSelect.Limit);

        return (transformedSelect, correlations);
    }

    /// <summary>
    /// Extracts correlations from a nested SELECT, handling both direct predicates
    /// and correlations in deeply nested tables (e.g., a SELECT containing another SELECT with correlations).
    /// </summary>
    /// <returns>
    /// A tuple containing:
    /// - transformedSelect: The SELECT with correlated predicates removed
    /// - correlations: List of (outer column, inner column) pairs for building join conditions
    /// - directPredicates: List of predicates involving outer columns only (like NULL checks) to add directly to join
    /// </returns>
    private (SelectExpression transformedSelect, List<(ColumnExpression outer, SqlExpression inner)> correlations, List<SqlExpression> directPredicates)?
        ExtractCorrelationsFromNestedSelectDeep(SelectExpression nestedSelect)
    {
        DebugLog($"        ExtractCorrelationsFromNestedSelectDeep: alias={nestedSelect.Alias}");
        DebugLog($"          Predicate: {DebugPrintExpression(nestedSelect.Predicate)}");
        DebugLog($"          OuterAliases: [{string.Join(",", _outerTableAliases)}]");
        DebugLog($"          AncestorAliases: [{string.Join(",", _ancestorAliases)}]");
        DebugLog($"          Tables: {string.Join(", ", nestedSelect.Tables.Select(t => t.GetType().Name + ":" + t.Alias))}");

        var correlations = new List<(ColumnExpression outer, SqlExpression inner)>();
        var additionalProjections = new List<ProjectionExpression>();
        var projectionMapping = new Dictionary<ColumnExpression, ColumnExpression>();

        // First, recursively extract correlations from nested tables
        var (transformedTables, nestedCorrelations) = ExtractNestedCorrelations(nestedSelect.Tables);
        DebugLog($"          Nested correlations from tables: {nestedCorrelations.Count}");

        // Project nested correlations through this SELECT
        foreach (var (outerCol, innerExpr) in nestedCorrelations)
        {
            if (innerExpr is ColumnExpression innerCol)
            {
                // If this SELECT has GROUP BY, we need to add the column to GROUP BY as well
                EnsureColumnProjected(nestedSelect, innerCol, additionalProjections, projectionMapping);

                if (projectionMapping.TryGetValue(innerCol, out var projectedCol))
                {
                    correlations.Add((outerCol, projectedCol));
                }
            }
        }

        // Then handle direct predicates in this SELECT
        var hasOuterRef = nestedSelect.Predicate != null && ContainsOuterReference(nestedSelect.Predicate);
        DebugLog($"          Predicate contains outer reference: {hasOuterRef}");
        if (nestedSelect.Predicate != null && hasOuterRef)
        {
            var (correlatedParts, nonCorrelatedParts, complexCorrelatedParts) = SplitPredicate(nestedSelect.Predicate);
            DebugLog($"          SplitPredicate: correlated={correlatedParts.Count}, nonCorr={nonCorrelatedParts.Count}, complex={complexCorrelatedParts.Count}");

            // Separate outer-only predicates (like `outer_col <> NULL`) from truly complex predicates
            var outerOnlyPredicates = new List<SqlExpression>();
            var trulyComplexPredicates = new List<SqlExpression>();

            foreach (var complexPred in complexCorrelatedParts)
            {
                if (IsOuterOnlyPredicate(complexPred))
                {
                    outerOnlyPredicates.Add(complexPred);
                }
                else
                {
                    trulyComplexPredicates.Add(complexPred);
                }
            }

            DebugLog($"          Outer-only predicates: {outerOnlyPredicates.Count}, truly complex: {trulyComplexPredicates.Count}");

            // Can't handle truly complex correlated predicates
            if (trulyComplexPredicates.Count > 0)
            {
                DebugLog($"          Returning null due to truly complex correlated predicates");
                return null;
            }

            // We need a separate list for join predicates that don't require projection
            var directJoinPredicates = outerOnlyPredicates.ToList();

            foreach (var correlation in correlatedParts)
            {
                DebugLog($"          Processing correlation: outer={correlation.OuterCol}, inner type={correlation.InnerExpr.GetType().Name}");
                if (correlation.InnerExpr is ColumnExpression innerCol)
                {
                    DebugLog($"            Inner column: {innerCol.TableAlias}.{innerCol.Name}");
                    EnsureColumnProjected(nestedSelect, innerCol, additionalProjections, projectionMapping);

                    if (projectionMapping.TryGetValue(innerCol, out var projectedCol))
                    {
                        DebugLog($"            Added correlation to {projectedCol.TableAlias}.{projectedCol.Name}");
                        correlations.Add((correlation.OuterCol, projectedCol));
                    }
                    else
                    {
                        DebugLog($"          Returning null: projection mapping not found for {innerCol}");
                        return null;
                    }
                }
                else if (correlation.InnerExpr is SqlConstantExpression)
                {
                    // Inner expression is a constant (e.g., NULL from nullability processor)
                    // This correlation (like `outer_col <> NULL`) should be added directly to join condition
                    DebugLog($"            Inner is constant, adding to direct join predicates");
                    directJoinPredicates.Add(correlation.OriginalPredicate);
                }
                else
                {
                    DebugLog($"          Returning null: inner expr not a column or constant, type={correlation.InnerExpr.GetType().Name}");
                    return null;
                }
            }

            // Build the new predicate without correlated parts
            SqlExpression? newPredicate = null;
            foreach (var pred in nonCorrelatedParts)
            {
                newPredicate = newPredicate == null ? pred : _sqlExpressionFactory.AndAlso(newPredicate, pred);
            }

            // Build updated projections
            var newProjections = nestedSelect.Projection.ToList();
            newProjections.AddRange(additionalProjections);

            // If this SELECT has GROUP BY and we added projections, we need to add them to GROUP BY too
            var newGroupBy = nestedSelect.GroupBy.ToList();
            if (nestedSelect.GroupBy.Count > 0 && additionalProjections.Count > 0)
            {
                foreach (var addedProj in additionalProjections)
                {
                    // Add the column to GROUP BY (the actual expression, not the projection)
                    if (addedProj.Expression is ColumnExpression col)
                    {
                        newGroupBy.Add(col);
                    }
                }
            }

            var transformedSelect = nestedSelect.Update(
                transformedTables ?? nestedSelect.Tables,
                newPredicate,
                newGroupBy,
                nestedSelect.Having,
                newProjections,
                nestedSelect.Orderings,
                nestedSelect.Offset,
                nestedSelect.Limit);

            DebugLog($"          Returning: {correlations.Count} correlations, {directJoinPredicates.Count} direct predicates");
            return (transformedSelect, correlations, directJoinPredicates.Cast<SqlExpression>().ToList());
        }

        // No direct predicate correlations, but we might have nested correlations
        if (correlations.Count == 0 && transformedTables == null)
        {
            return null;
        }

        // Build updated projections
        var finalProjections = nestedSelect.Projection.ToList();
        finalProjections.AddRange(additionalProjections);

        // If this SELECT has GROUP BY and we added projections, we need to add them to GROUP BY too
        var finalGroupBy = nestedSelect.GroupBy.ToList();
        if (nestedSelect.GroupBy.Count > 0 && additionalProjections.Count > 0)
        {
            foreach (var addedProj in additionalProjections)
            {
                if (addedProj.Expression is ColumnExpression col)
                {
                    finalGroupBy.Add(col);
                }
            }
        }

        var result = nestedSelect.Update(
            transformedTables ?? nestedSelect.Tables,
            nestedSelect.Predicate,
            finalGroupBy,
            nestedSelect.Having,
            finalProjections,
            nestedSelect.Orderings,
            nestedSelect.Offset,
            nestedSelect.Limit);

        return (result, correlations, new List<SqlExpression>());
    }

    // Correlation info: the outer column, the inner expression, and the original predicate
    private record CorrelationInfo(ColumnExpression OuterCol, SqlExpression InnerExpr, SqlBinaryExpression OriginalPredicate);

    private (List<CorrelationInfo> correlations,
             List<SqlExpression> nonCorrelated,
             List<SqlExpression> complexCorrelated) SplitPredicate(SqlExpression predicate)
    {
        var correlations = new List<CorrelationInfo>();
        var nonCorrelated = new List<SqlExpression>();
        var complexCorrelated = new List<SqlExpression>();

        SplitPredicateRecursive(predicate, correlations, nonCorrelated, complexCorrelated);

        return (correlations, nonCorrelated, complexCorrelated);
    }

    // Helper to convert CorrelationInfo to simple tuple for compatibility
    private static List<(ColumnExpression outer, SqlExpression inner)> ToSimpleTuples(List<CorrelationInfo> correlations)
    {
        return correlations.Select(c => (c.OuterCol, c.InnerExpr)).ToList();
    }

    // Create a comparison expression with the same operator as the original
    private SqlExpression CreateComparisonWithSameOperator(
        SqlBinaryExpression original, SqlExpression left, SqlExpression right)
    {
        return original.OperatorType switch
        {
            ExpressionType.Equal => _sqlExpressionFactory.Equal(left, right),
            ExpressionType.NotEqual => _sqlExpressionFactory.NotEqual(left, right),
            ExpressionType.LessThan => _sqlExpressionFactory.LessThan(left, right),
            ExpressionType.LessThanOrEqual => _sqlExpressionFactory.LessThanOrEqual(left, right),
            ExpressionType.GreaterThan => _sqlExpressionFactory.GreaterThan(left, right),
            ExpressionType.GreaterThanOrEqual => _sqlExpressionFactory.GreaterThanOrEqual(left, right),
            _ => _sqlExpressionFactory.Equal(left, right) // Fallback to Equal
        };
    }

    private void SplitPredicateRecursive(
        SqlExpression predicate,
        List<CorrelationInfo> correlations,
        List<SqlExpression> nonCorrelated,
        List<SqlExpression> complexCorrelated)
    {
        if (predicate is SqlBinaryExpression { OperatorType: ExpressionType.AndAlso } andExpr)
        {
            SplitPredicateRecursive(andExpr.Left, correlations, nonCorrelated, complexCorrelated);
            SplitPredicateRecursive(andExpr.Right, correlations, nonCorrelated, complexCorrelated);
            return;
        }

        // Check if predicate references outer tables
        if (!ContainsOuterReference(predicate))
        {
            nonCorrelated.Add(predicate);
            return;
        }

        // Has outer references - check if it's a simple comparison we can extract
        // Handle Equal, NotEqual, and other comparison operators
        if (predicate is SqlBinaryExpression binExpr &&
            (binExpr.OperatorType == ExpressionType.Equal ||
             binExpr.OperatorType == ExpressionType.NotEqual ||
             binExpr.OperatorType == ExpressionType.LessThan ||
             binExpr.OperatorType == ExpressionType.LessThanOrEqual ||
             binExpr.OperatorType == ExpressionType.GreaterThan ||
             binExpr.OperatorType == ExpressionType.GreaterThanOrEqual))
        {

            if (binExpr.Left is ColumnExpression leftCol && _outerTableAliases.Contains(leftCol.TableAlias))
            {
                if (!ContainsOuterReference(binExpr.Right))
                {
                    // Check if the right side has column references (not just a constant)
                    // If it's a constant, add to complexCorrelated as it needs special handling
                    if (HasColumnReference(binExpr.Right))
                    {
                        correlations.Add(new CorrelationInfo(leftCol, binExpr.Right, binExpr));
                        return;
                    }
                    // else: outer_col op constant - this is an outer-only predicate
                    // Fall through to add to complexCorrelated
                }
            }

            if (binExpr.Right is ColumnExpression rightCol && _outerTableAliases.Contains(rightCol.TableAlias))
            {
                if (!ContainsOuterReference(binExpr.Left))
                {
                    // Check if the left side has column references (not just a constant)
                    if (HasColumnReference(binExpr.Left))
                    {
                        correlations.Add(new CorrelationInfo(rightCol, binExpr.Left, binExpr));
                        return;
                    }
                    // else: constant op outer_col - this is an outer-only predicate
                    // Fall through to add to complexCorrelated
                }
            }

        }

        complexCorrelated.Add(predicate);
    }

    /// <summary>
    /// Checks if an expression contains any column references (vs being just a constant).
    /// </summary>
    private static bool HasColumnReference(SqlExpression expression)
    {
        if (expression is ColumnExpression)
            return true;
        if (expression is SqlBinaryExpression bin)
            return HasColumnReference(bin.Left) || HasColumnReference(bin.Right);
        if (expression is SqlUnaryExpression un)
            return HasColumnReference(un.Operand);
        return false;
    }

    /// <summary>
    /// Checks if a predicate only involves outer columns and constants (no inner column references).
    /// Such predicates can be added directly to the join condition without projection.
    /// Example: u.FullName IS NOT NULL, u.FullName &lt;&gt; 'test'
    /// </summary>
    private bool IsOuterOnlyPredicate(SqlExpression predicate)
    {
        if (predicate is SqlBinaryExpression binExpr)
        {
            var leftIsOuterOrConst = IsOuterOrConstant(binExpr.Left);
            var rightIsOuterOrConst = IsOuterOrConstant(binExpr.Right);
            return leftIsOuterOrConst && rightIsOuterOrConst;
        }
        if (predicate is SqlUnaryExpression unExpr)
        {
            return IsOuterOrConstant(unExpr.Operand);
        }
        return false;
    }

    /// <summary>
    /// Checks if an expression is either an outer column reference or a constant.
    /// </summary>
    private bool IsOuterOrConstant(SqlExpression expression)
    {
        if (expression is SqlConstantExpression)
            return true;
        if (expression is ColumnExpression col)
            return _outerTableAliases.Contains(col.TableAlias);
        if (expression is SqlUnaryExpression un)
            return IsOuterOrConstant(un.Operand);
        return false;
    }

    private void EnsureColumnProjected(
        SelectExpression select,
        ColumnExpression column,
        List<ProjectionExpression> additionalProjections,
        Dictionary<ColumnExpression, ColumnExpression> projectionMapping)
    {
        if (projectionMapping.ContainsKey(column))
            return;

        // IMPORTANT: If the column references this SELECT's own alias, it's a self-reference.
        // This can happen when correlations are propagated through multiple levels and we
        // receive a column like "w1._corr_Name" where w1 is THIS SELECT's alias.
        // In this case, the column is already part of this SELECT's output - we shouldn't
        // try to project it again. Instead, find the matching projection by alias.
        if (column.TableAlias == select.Alias)
        {
            // The column is referencing this SELECT's output - find it by name
            foreach (var projection in select.Projection)
            {
                if (projection.Alias == column.Name)
                {
                    // Found it - just use this column as-is (it's already projected)
                    projectionMapping[column] = column;
                    return;
                }
            }
            // Also check additional projections
            foreach (var projection in additionalProjections)
            {
                if (projection.Alias == column.Name)
                {
                    projectionMapping[column] = column;
                    return;
                }
            }
            // If not found, this is an error - we're trying to reference output that doesn't exist
            DebugLog($"    WARNING: Self-reference to {column.TableAlias}.{column.Name} not found in projections");
            return;
        }

        // Check if column is already projected
        foreach (var projection in select.Projection)
        {
            if (projection.Expression is ColumnExpression projCol &&
                projCol.TableAlias == column.TableAlias &&
                projCol.Name == column.Name)
            {
                var projectedColumn = new ColumnExpression(
                    projection.Alias,
                    select.Alias!,
                    column.Type,
                    column.TypeMapping,
                    column.IsNullable);
                projectionMapping[column] = projectedColumn;
                return;
            }
        }

        // Check in additional projections we've already added
        foreach (var projection in additionalProjections)
        {
            if (projection.Expression is ColumnExpression projCol &&
                projCol.TableAlias == column.TableAlias &&
                projCol.Name == column.Name)
            {
                var projectedColumn = new ColumnExpression(
                    projection.Alias,
                    select.Alias!,
                    column.Type,
                    column.TypeMapping,
                    column.IsNullable);
                projectionMapping[column] = projectedColumn;
                return;
            }
        }

        // Need to add projection
        var projectionAlias = $"_corr_{column.Name}";
        var newProjection = new ProjectionExpression(column, projectionAlias);
        additionalProjections.Add(newProjection);

        var newProjectedColumn = new ColumnExpression(
            projectionAlias,
            select.Alias!,
            column.Type,
            column.TypeMapping,
            column.IsNullable);
        projectionMapping[column] = newProjectedColumn;
    }

    private ColumnExpression RemapColumnToSelectAlias(ColumnExpression column, SelectExpression select)
    {
        foreach (var projection in select.Projection)
        {
            if (projection.Expression is ColumnExpression projCol &&
                projCol.TableAlias == column.TableAlias &&
                projCol.Name == column.Name)
            {
                return new ColumnExpression(
                    projection.Alias,
                    select.Alias!,
                    column.Type,
                    column.TypeMapping,
                    column.IsNullable);
            }
        }

        return column;
    }

    private List<ColumnExpression> FindInnerColumns(SqlExpression expression)
    {
        var finder = new InnerColumnFinder(_outerTableAliases);
        finder.Visit(expression);
        return finder.InnerColumns;
    }

    private SqlExpression RemapPredicateColumns(
        SqlExpression predicate,
        SelectExpression select,
        Dictionary<ColumnExpression, ColumnExpression> projectionMapping)
    {
        return (SqlExpression)new ColumnRemapper(select, projectionMapping, _outerTableAliases).Visit(predicate);
    }

    private record TransformResult(SelectExpression TransformedSelect, SqlExpression? JoinPredicate);

    private class OuterColumnFinder : ExpressionVisitor
    {
        private readonly HashSet<string> _outerTableAliases;
        public HashSet<ColumnExpression> OuterColumns { get; } = new();

        public OuterColumnFinder(HashSet<string> outerTableAliases) => _outerTableAliases = outerTableAliases;

        protected override Expression VisitExtension(Expression node)
        {
            if (node is ColumnExpression column && _outerTableAliases.Contains(column.TableAlias))
                OuterColumns.Add(column);
            return base.VisitExtension(node);
        }
    }

    private class InnerColumnFinder : ExpressionVisitor
    {
        private readonly HashSet<string> _outerTableAliases;
        public List<ColumnExpression> InnerColumns { get; } = new();

        public InnerColumnFinder(HashSet<string> outerTableAliases) => _outerTableAliases = outerTableAliases;

        protected override Expression VisitExtension(Expression node)
        {
            if (node is ColumnExpression column && !_outerTableAliases.Contains(column.TableAlias))
                InnerColumns.Add(column);
            return base.VisitExtension(node);
        }
    }

    private class ColumnRemapper : ExpressionVisitor
    {
        private readonly SelectExpression _select;
        private readonly Dictionary<ColumnExpression, ColumnExpression> _projectionMapping;
        private readonly HashSet<string> _outerTableAliases;

        public ColumnRemapper(
            SelectExpression select,
            Dictionary<ColumnExpression, ColumnExpression> projectionMapping,
            HashSet<string> outerTableAliases)
        {
            _select = select;
            _projectionMapping = projectionMapping;
            _outerTableAliases = outerTableAliases;
        }

        protected override Expression VisitExtension(Expression node)
        {
            if (node is ColumnExpression column)
            {
                if (_outerTableAliases.Contains(column.TableAlias))
                    return column;

                if (_projectionMapping.TryGetValue(column, out var mappedColumn))
                    return mappedColumn;

                foreach (var projection in _select.Projection)
                {
                    if (projection.Expression is ColumnExpression projCol &&
                        projCol.TableAlias == column.TableAlias &&
                        projCol.Name == column.Name)
                    {
                        return new ColumnExpression(
                            projection.Alias,
                            _select.Alias!,
                            column.Type,
                            column.TypeMapping,
                            column.IsNullable);
                    }
                }
            }

            return base.VisitExtension(node);
        }
    }
}
