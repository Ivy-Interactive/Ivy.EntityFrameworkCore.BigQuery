using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
using System.Linq.Expressions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal;

#pragma warning disable EF1001 // Internal EF Core API usage.

/// <summary>
/// BigQuery doesn't support LATERAL joins or correlated subqueries in FROM clause.
/// This postprocessor transforms OUTER APPLY and CROSS APPLY expressions by extracting
/// correlated predicates from the inner subquery's WHERE clause and moving them to the JOIN condition.
///
/// For example:
/// FROM Officers AS u
/// OUTER APPLY (
///     SELECT g.FullName, g.Nickname
///     FROM Gears AS g
///     WHERE u.Nickname = g.LeaderNickname AND u.SquadId = g.LeaderSquadId
/// ) AS s
///
/// Becomes:
/// FROM Officers AS u
/// LEFT JOIN (
///     SELECT g.FullName, g.Nickname, g.LeaderNickname, g.LeaderSquadId
///     FROM Gears AS g
/// ) AS s ON u.Nickname = s.LeaderNickname AND u.SquadId = s.LeaderSquadId
/// </summary>
public class BigQueryApplyPostprocessor : ExpressionVisitor
{
    private readonly ISqlExpressionFactory _sqlExpressionFactory;
    private readonly IRelationalTypeMappingSource _typeMappingSource;

    // Track outer table aliases at each level of nesting
    private HashSet<string> _outerTableAliases = new();

    // Track correlated projection remappings: maps (joinAlias, columnName) to the outer column expression
    // When a CROSS APPLY inner SELECT has correlated projections like c.ContactName,
    // we need to remap outer references from o0.ContactName to c.ContactName
    private Dictionary<(string tableAlias, string columnName), ColumnExpression> _correlatedProjectionRemappings = new();

    public BigQueryApplyPostprocessor(
        ISqlExpressionFactory sqlExpressionFactory,
        IRelationalTypeMappingSource typeMappingSource)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
        _typeMappingSource = typeMappingSource;
    }

    protected override Expression VisitExtension(Expression node)
    {
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
        // Early out: if there are no APPLY expressions anywhere in this select's tables,
        // skip processing entirely to avoid inadvertent modifications
        if (!ContainsApplyExpression(select))
        {
            return select;
        }

        // Save current outer aliases and build new set including this select's tables
        var previousOuterAliases = _outerTableAliases;
        _outerTableAliases = new HashSet<string>(previousOuterAliases);

        // Process tables, transforming APPLY expressions as we go
        var newTables = new List<TableExpressionBase>();
        var tablesChanged = false;

        foreach (var table in select.Tables)
        {
            // Add non-join table aliases to outer aliases before processing subsequent tables
            var tableAlias = table.UnwrapJoin().Alias;
            if (tableAlias != null && table is not JoinExpressionBase)
            {
                _outerTableAliases.Add(tableAlias);
            }

            var newTable = VisitTableExpression(table);
            newTables.Add(newTable);

            if (newTable != table)
            {
                tablesChanged = true;
            }

            // Add the alias after processing (for join expressions)
            if (tableAlias != null && table is JoinExpressionBase)
            {
                _outerTableAliases.Add(tableAlias);
            }
        }

        // Apply correlated projection remappings to other parts of the select
        // These remappings are populated when transforming CROSS/OUTER APPLY with correlated projections
        var remapper = _correlatedProjectionRemappings.Count > 0
            ? new CorrelatedColumnRemapper(_correlatedProjectionRemappings)
            : null;

        // Visit and potentially remap other parts of the select
        var newPredicate = select.Predicate != null ? (SqlExpression?)Visit(select.Predicate) : null;
        if (newPredicate != null && remapper != null)
            newPredicate = (SqlExpression)remapper.Visit(newPredicate);

        var newHaving = select.Having != null ? (SqlExpression?)Visit(select.Having) : null;
        if (newHaving != null && remapper != null)
            newHaving = (SqlExpression)remapper.Visit(newHaving);

        var newProjections = select.Projection.Select(p =>
        {
            var visited = (ProjectionExpression)Visit(p);
            return remapper != null ? (ProjectionExpression)remapper.Visit(visited) : visited;
        }).ToList();

        var newGroupBy = select.GroupBy.Select(g =>
        {
            var visited = (SqlExpression)Visit(g);
            return remapper != null ? (SqlExpression)remapper.Visit(visited) : visited;
        }).ToList();

        var newOrderings = select.Orderings.Select(o =>
        {
            var visited = (OrderingExpression)Visit(o);
            return remapper != null ? (OrderingExpression)remapper.Visit(visited) : visited;
        }).ToList();

        var newOffset = select.Offset != null ? (SqlExpression?)Visit(select.Offset) : null;
        var newLimit = select.Limit != null ? (SqlExpression?)Visit(select.Limit) : null;

        // Clear remappings for this level
        _correlatedProjectionRemappings.Clear();

        _outerTableAliases = previousOuterAliases;

        // Check if anything changed
        var projectionsChanged = !newProjections.SequenceEqual(select.Projection);
        var groupByChanged = !newGroupBy.SequenceEqual(select.GroupBy);
        var orderingsChanged = !newOrderings.SequenceEqual(select.Orderings);

        if (tablesChanged || newPredicate != select.Predicate || newHaving != select.Having ||
            projectionsChanged || groupByChanged || orderingsChanged ||
            newOffset != select.Offset || newLimit != select.Limit)
        {
            return select.Update(
                newTables,
                newPredicate,
                newGroupBy,
                newHaving,
                newProjections,
                newOrderings,
                newOffset,
                newLimit);
        }

        return select;
    }

    private TableExpressionBase VisitTableExpression(TableExpressionBase table)
    {
        return table switch
        {
            OuterApplyExpression outerApply => TransformOuterApply(outerApply),
            CrossApplyExpression crossApply => TransformCrossApply(crossApply),
            LeftJoinExpression leftJoin => VisitLeftJoin(leftJoin),
            InnerJoinExpression innerJoin => VisitInnerJoin(innerJoin),
            CrossJoinExpression crossJoin => VisitCrossJoin(crossJoin),
            SelectExpression nestedSelect => (TableExpressionBase)VisitSelect(nestedSelect),
            _ => table
        };
    }

    private TableExpressionBase VisitLeftJoin(LeftJoinExpression leftJoin)
    {
        var newTable = VisitTableExpression(leftJoin.Table);
        var newPredicate = (SqlExpression)Visit(leftJoin.JoinPredicate);

        if (newTable != leftJoin.Table || newPredicate != leftJoin.JoinPredicate)
        {
            return new LeftJoinExpression(newTable, newPredicate, leftJoin.IsPrunable);
        }

        return leftJoin;
    }

    private TableExpressionBase VisitInnerJoin(InnerJoinExpression innerJoin)
    {
        var newTable = VisitTableExpression(innerJoin.Table);
        var newPredicate = (SqlExpression)Visit(innerJoin.JoinPredicate);

        if (newTable != innerJoin.Table || newPredicate != innerJoin.JoinPredicate)
        {
            return new InnerJoinExpression(newTable, newPredicate, innerJoin.IsPrunable);
        }

        return innerJoin;
    }

    private TableExpressionBase VisitCrossJoin(CrossJoinExpression crossJoin)
    {
        var newTable = VisitTableExpression(crossJoin.Table);

        if (newTable != crossJoin.Table)
        {
            return new CrossJoinExpression(newTable);
        }

        return crossJoin;
    }

    private TableExpressionBase TransformOuterApply(OuterApplyExpression outerApply)
    {
        if (outerApply.Table is not SelectExpression innerSelect)
        {
            // If not a SelectExpression, just return as-is (SQL generator will handle it)
            return outerApply;
        }

        // Identify correlated projections - these need special handling
        // Same logic as TransformCrossApply
        var correlatedProjections = new List<(ProjectionExpression projection, ColumnExpression outerColumn)>();
        var nonCorrelatedProjections = new List<ProjectionExpression>();

        foreach (var projection in innerSelect.Projection)
        {
            if (projection.Expression is ColumnExpression column && _outerTableAliases.Contains(column.TableAlias))
            {
                // This projection is a direct outer column reference (e.g., c.ContactName)
                // We'll remap references to this from the outer SELECT
                correlatedProjections.Add((projection, column));
            }
            else if (ContainsOuterReference(projection.Expression))
            {
                // Complex correlated projection (expression involving outer columns)
                // We can't handle this - bail out
                return outerApply;
            }
            else
            {
                nonCorrelatedProjections.Add(projection);
            }
        }

        // Now recursively process the inner table
        var processedInnerTable = VisitTableExpression(outerApply.Table);

        if (processedInnerTable is not SelectExpression processedInnerSelect)
        {
            // Shouldn't happen since we checked above, but handle gracefully
            return outerApply;
        }

        // If we have correlated projections, rebuild the inner SELECT without them
        // and set up remappings for the outer SELECT
        if (correlatedProjections.Count > 0)
        {
            // Get the join alias (this will be the alias of the transformed join result)
            var joinAlias = innerSelect.Alias!;

            // Register remappings: outer SELECT references to joinAlias.columnName -> outerColumn
            foreach (var (projection, outerColumn) in correlatedProjections)
            {
                _correlatedProjectionRemappings[(joinAlias, projection.Alias)] = outerColumn;
            }

            // Rebuild inner SELECT with only non-correlated projections
            // If all projections are correlated, we need to add a dummy projection
            var projectionsToUse = nonCorrelatedProjections.Count > 0
                ? nonCorrelatedProjections
                : [new ProjectionExpression(_sqlExpressionFactory.Constant(1, _typeMappingSource.FindMapping(typeof(int))), "_dummy")];

            processedInnerSelect = processedInnerSelect.Update(
                processedInnerSelect.Tables.ToList(),
                processedInnerSelect.Predicate,
                processedInnerSelect.GroupBy.ToList(),
                processedInnerSelect.Having,
                projectionsToUse,
                processedInnerSelect.Orderings.ToList(),
                processedInnerSelect.Offset,
                processedInnerSelect.Limit);
        }

        // Analyze for correlated predicates
        var result = ExtractCorrelatedPredicates(processedInnerSelect);

        if (result == null || result.JoinPredicate == null)
        {
            // No correlated predicates found - but if we had correlated projections, we still need to transform
            if (correlatedProjections.Count > 0)
            {
                // Create a simple TRUE predicate for the join
                var truePredicate = _sqlExpressionFactory.Constant(true);
                return new LeftJoinExpression(processedInnerSelect, truePredicate, prunable: false);
            }
            // No correlated predicates and no correlated projections - return original
            return outerApply;
        }

        return new LeftJoinExpression(result.TransformedSelect, result.JoinPredicate, prunable: false);
    }

    private TableExpressionBase TransformCrossApply(CrossApplyExpression crossApply)
    {
        if (crossApply.Table is not SelectExpression innerSelect)
        {
            // If not a SelectExpression, return as-is (SQL generator will handle it)
            return crossApply;
        }

        // Identify correlated projections - these need special handling
        var correlatedProjections = new List<(ProjectionExpression projection, ColumnExpression outerColumn)>();
        var nonCorrelatedProjections = new List<ProjectionExpression>();

        foreach (var projection in innerSelect.Projection)
        {
            if (projection.Expression is ColumnExpression column && _outerTableAliases.Contains(column.TableAlias))
            {
                // This projection is a direct outer column reference (e.g., c.ContactName)
                // We'll remap references to this from the outer SELECT
                correlatedProjections.Add((projection, column));
            }
            else if (ContainsOuterReference(projection.Expression))
            {
                // Complex correlated projection (expression involving outer columns)
                // We can't handle this - bail out
                return crossApply;
            }
            else
            {
                nonCorrelatedProjections.Add(projection);
            }
        }

        // Now recursively process the inner table
        var processedInnerTable = VisitTableExpression(crossApply.Table);

        if (processedInnerTable is not SelectExpression processedInnerSelect)
        {
            // Shouldn't happen since we checked above, but handle gracefully
            return crossApply;
        }

        // If we have correlated projections, rebuild the inner SELECT without them
        // and set up remappings for the outer SELECT
        if (correlatedProjections.Count > 0)
        {
            // Get the join alias (this will be the alias of the transformed join result)
            var joinAlias = innerSelect.Alias!;

            // Register remappings: outer SELECT references to joinAlias.columnName -> outerColumn
            foreach (var (projection, outerColumn) in correlatedProjections)
            {
                _correlatedProjectionRemappings[(joinAlias, projection.Alias)] = outerColumn;
            }

            // Rebuild inner SELECT with only non-correlated projections
            // If all projections are correlated, we need to add a dummy projection
            var projectionsToUse = nonCorrelatedProjections.Count > 0
                ? nonCorrelatedProjections
                : [new ProjectionExpression(_sqlExpressionFactory.Constant(1, _typeMappingSource.FindMapping(typeof(int))), "_dummy")];

            processedInnerSelect = processedInnerSelect.Update(
                processedInnerSelect.Tables.ToList(),
                processedInnerSelect.Predicate,
                processedInnerSelect.GroupBy.ToList(),
                processedInnerSelect.Having,
                projectionsToUse,
                processedInnerSelect.Orderings.ToList(),
                processedInnerSelect.Offset,
                processedInnerSelect.Limit);
        }

        // Analyze for correlated predicates
        var result = ExtractCorrelatedPredicates(processedInnerSelect);

        if (result == null || result.JoinPredicate == null)
        {
            // No correlated predicates found - but if we had correlated projections, we still need to transform
            if (correlatedProjections.Count > 0)
            {
                // Create a simple TRUE predicate for the join
                var truePredicate = _sqlExpressionFactory.Constant(true);
                return new InnerJoinExpression(processedInnerSelect, truePredicate, prunable: false);
            }
            // No correlated predicates and no correlated projections - return original
            return crossApply;
        }

        return new InnerJoinExpression(result.TransformedSelect, result.JoinPredicate, prunable: false);
    }

    private ApplyTransformResult? ExtractCorrelatedPredicates(SelectExpression innerSelect)
    {
        // First, check if projections contain outer references - if so, we can't transform
        foreach (var projection in innerSelect.Projection)
        {
            if (ContainsOuterReference(projection.Expression))
            {
                return null; // Can't transform - outer reference in projection
            }
        }

        // Check if nested tables (JOINs, subqueries) contain outer references
        // If so, we need to handle them recursively
        var (transformedTables, nestedCorrelations) = ExtractNestedCorrelations(innerSelect.Tables);

        // Combine predicates from nested extractions with the main predicate
        SqlExpression? combinedPredicate = innerSelect.Predicate;

        // If we have no predicate and no nested correlations, nothing to transform
        if (combinedPredicate == null && nestedCorrelations.Count == 0)
        {
            return null;
        }

        // Find all column references to outer tables in the combined predicate
        var outerColumnFinder = new OuterColumnFinder(_outerTableAliases);
        if (combinedPredicate != null)
        {
            outerColumnFinder.Visit(combinedPredicate);
        }

        if (outerColumnFinder.OuterColumns.Count == 0 && nestedCorrelations.Count == 0)
        {
            // No outer column references - nothing to transform
            return null;
        }

        // Split the predicate into correlated (references outer tables) and non-correlated parts
        var (correlatedParts, nonCorrelatedParts, complexCorrelatedParts) = combinedPredicate != null
            ? SplitPredicate(combinedPredicate)
            : (new List<(ColumnExpression, SqlExpression)>(), new List<SqlExpression>(), new List<SqlExpression>());

        // Note: nestedCorrelations are already remapped columns from nested selects
        // They should NOT be processed by EnsureColumnProjected since they're already projected

        if (correlatedParts.Count == 0 && complexCorrelatedParts.Count == 0 && nestedCorrelations.Count == 0)
        {
            return null;
        }

        // For complex correlated predicates (not simple equality), we need to ensure
        // the referenced inner columns are projected
        var additionalProjections = new List<ProjectionExpression>();
        var projectionMapping = new Dictionary<ColumnExpression, ColumnExpression>();

        // Process equality correlations - ensure inner columns are projected
        // Only process direct correlations, not nested ones (which are already remapped)
        foreach (var (outerCol, innerExpr) in correlatedParts)
        {
            if (innerExpr is ColumnExpression innerCol)
            {
                EnsureColumnProjected(innerSelect, innerCol, additionalProjections, projectionMapping);
            }
        }

        // Process complex correlations - ensure all inner columns are projected
        foreach (var complexPred in complexCorrelatedParts)
        {
            var innerColumns = FindInnerColumns(complexPred, innerSelect);
            foreach (var innerCol in innerColumns)
            {
                EnsureColumnProjected(innerSelect, innerCol, additionalProjections, projectionMapping);
            }
        }

        // Build the new inner SELECT with additional projections, remaining predicate, and transformed tables
        SelectExpression newInnerSelect;

        var predicateCount = combinedPredicate != null ? GetPredicateCount(combinedPredicate) : 0;
        var tablesChanged = transformedTables != null;
        var needsUpdate = additionalProjections.Count > 0 || nonCorrelatedParts.Count < predicateCount || tablesChanged;

        if (needsUpdate)
        {
            var newProjections = innerSelect.Projection.ToList();
            newProjections.AddRange(additionalProjections);

            SqlExpression? newPredicate = null;
            foreach (var pred in nonCorrelatedParts)
            {
                newPredicate = newPredicate == null ? pred : _sqlExpressionFactory.AndAlso(newPredicate, pred);
            }

            var tablesToUse = transformedTables ?? innerSelect.Tables;

            // If this SELECT has GROUP BY and we added correlation projections,
            // we need to also add those columns to the GROUP BY clause
            var newGroupBy = innerSelect.GroupBy.ToList();
            if (innerSelect.GroupBy.Count > 0 && additionalProjections.Count > 0)
            {
                foreach (var addedProj in additionalProjections)
                {
                    if (addedProj.Expression is ColumnExpression col)
                    {
                        // Check if not already in GROUP BY
                        var alreadyInGroupBy = newGroupBy.Any(g =>
                            g is ColumnExpression gc &&
                            gc.TableAlias == col.TableAlias &&
                            gc.Name == col.Name);
                        if (!alreadyInGroupBy)
                        {
                            newGroupBy.Add(col);
                        }
                    }
                }
            }

            newInnerSelect = innerSelect.Update(
                tablesToUse,
                newPredicate,
                newGroupBy,
                innerSelect.Having,
                newProjections,
                innerSelect.Orderings,
                innerSelect.Offset,
                innerSelect.Limit);
        }
        else
        {
            newInnerSelect = innerSelect;
        }

        // Build the join predicate from correlated parts
        SqlExpression? joinPredicate = null;

        // Add equality correlations (from direct predicate analysis)
        foreach (var (outerCol, innerExpr) in correlatedParts)
        {
            SqlExpression joinCondition;

            if (innerExpr is ColumnExpression innerCol && projectionMapping.TryGetValue(innerCol, out var projectedCol))
            {
                joinCondition = _sqlExpressionFactory.Equal(outerCol, projectedCol);
            }
            else if (innerExpr is ColumnExpression directInnerCol)
            {
                // Column is already projected or accessible
                var remappedCol = RemapColumnToSelectAlias(directInnerCol, newInnerSelect);
                joinCondition = _sqlExpressionFactory.Equal(outerCol, remappedCol);
            }
            else
            {
                // Expression - wrap in subquery reference if needed
                joinCondition = _sqlExpressionFactory.Equal(outerCol, innerExpr);
            }

            joinPredicate = joinPredicate == null ? joinCondition : _sqlExpressionFactory.AndAlso(joinPredicate, joinCondition);
        }

        // Add nested correlations - these are already remapped columns from nested selects
        foreach (var (outerCol, innerExpr) in nestedCorrelations)
        {
            var joinCondition = _sqlExpressionFactory.Equal(outerCol, innerExpr);
            joinPredicate = joinPredicate == null ? joinCondition : _sqlExpressionFactory.AndAlso(joinPredicate, joinCondition);
        }

        // Add complex correlated predicates (remapped to use projected columns)
        foreach (var complexPred in complexCorrelatedParts)
        {
            var remappedPred = RemapPredicateColumns(complexPred, newInnerSelect, projectionMapping);
            joinPredicate = joinPredicate == null ? remappedPred : _sqlExpressionFactory.AndAlso(joinPredicate, remappedPred);
        }

        return new ApplyTransformResult(newInnerSelect, joinPredicate);
    }

    /// <summary>
    /// Extracts correlations from nested tables (JOINs with subqueries that reference outer tables).
    /// Returns transformed tables with correlations removed, and the extracted correlation predicates.
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

    /// <summary>
    /// Extracts correlations from a single table expression.
    /// </summary>
    private (TableExpressionBase table, List<(ColumnExpression outer, SqlExpression inner)> correlations)
        ExtractCorrelationsFromTable(TableExpressionBase table)
    {
        var correlations = new List<(ColumnExpression outer, SqlExpression inner)>();

        switch (table)
        {
            case InnerJoinExpression innerJoin when innerJoin.Table is SelectExpression nestedSelect:
                return ExtractCorrelationsFromJoinedSelect(innerJoin, nestedSelect, isLeftJoin: false);

            case LeftJoinExpression leftJoin when leftJoin.Table is SelectExpression nestedSelect:
                return ExtractCorrelationsFromJoinedSelect(leftJoin, nestedSelect, isLeftJoin: true);

            case SelectExpression nestedSelect:
                // Direct nested select (not wrapped in join) - check for correlations
                if (nestedSelect.Predicate != null && ContainsOuterReference(nestedSelect.Predicate))
                {
                    var result = ExtractCorrelatedPredicatesFromNestedSelect(nestedSelect);
                    if (result != null)
                    {
                        return (result.Value.transformedSelect, result.Value.correlations);
                    }
                }
                break;
        }

        return (table, correlations);
    }

    private (TableExpressionBase table, List<(ColumnExpression outer, SqlExpression inner)> correlations)
        ExtractCorrelationsFromJoinedSelect(PredicateJoinExpressionBase join, SelectExpression nestedSelect, bool isLeftJoin)
    {
        var correlations = new List<(ColumnExpression outer, SqlExpression inner)>();

        // Check if the nested select's predicate contains outer references
        if (nestedSelect.Predicate == null || !ContainsOuterReference(nestedSelect.Predicate))
        {
            // Also check the join predicate and nested tables
            if (!ContainsOuterReference(join.JoinPredicate))
            {
                // Check nested tables recursively
                var (transformedNestedTables, nestedCorrelations) = ExtractNestedCorrelations(nestedSelect.Tables);
                if (transformedNestedTables != null && nestedCorrelations.Count > 0)
                {
                    // Update the nested select with transformed tables
                    var updatedNestedSelect = nestedSelect.Update(
                        transformedNestedTables,
                        nestedSelect.Predicate,
                        nestedSelect.GroupBy,
                        nestedSelect.Having,
                        nestedSelect.Projection,
                        nestedSelect.Orderings,
                        nestedSelect.Offset,
                        nestedSelect.Limit);

                    var newJoin = isLeftJoin
                        ? (TableExpressionBase)new LeftJoinExpression(updatedNestedSelect, join.JoinPredicate, join.IsPrunable)
                        : new InnerJoinExpression(updatedNestedSelect, join.JoinPredicate, join.IsPrunable);

                    return (newJoin, nestedCorrelations);
                }
                return (join, correlations);
            }
        }

        // Extract correlations from the nested select
        var result = ExtractCorrelatedPredicatesFromNestedSelect(nestedSelect);
        if (result == null)
        {
            return (join, correlations);
        }

        var (transformedSelect, extractedCorrelations) = result.Value;
        correlations.AddRange(extractedCorrelations);

        // Create a new join with the transformed select
        var transformedJoin = isLeftJoin
            ? (TableExpressionBase)new LeftJoinExpression(transformedSelect, join.JoinPredicate, join.IsPrunable)
            : new InnerJoinExpression(transformedSelect, join.JoinPredicate, join.IsPrunable);

        return (transformedJoin, correlations);
    }

    private (SelectExpression transformedSelect, List<(ColumnExpression outer, SqlExpression inner)> correlations)?
        ExtractCorrelatedPredicatesFromNestedSelect(SelectExpression nestedSelect)
    {
        if (nestedSelect.Predicate == null)
        {
            return null;
        }

        // Check for outer references in projections - can't handle that
        foreach (var projection in nestedSelect.Projection)
        {
            if (ContainsOuterReference(projection.Expression))
            {
                return null;
            }
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

        foreach (var (outerCol, innerExpr) in correlatedParts)
        {
            if (innerExpr is ColumnExpression innerCol)
            {
                EnsureColumnProjected(nestedSelect, innerCol, additionalProjections, projectionMapping);

                if (projectionMapping.TryGetValue(innerCol, out var projectedCol))
                {
                    correlations.Add((outerCol, projectedCol));
                }
                else
                {
                    // Couldn't project - bail out
                    return null;
                }
            }
            else
            {
                // Non-column inner expression - not supported for nested extractions
                return null;
            }
        }

        // Build the new predicate without correlated parts
        SqlExpression? newPredicate = null;
        foreach (var pred in nonCorrelatedParts)
        {
            newPredicate = newPredicate == null ? pred : _sqlExpressionFactory.AndAlso(newPredicate, pred);
        }

        // Build the new projections
        var newProjections = nestedSelect.Projection.ToList();
        newProjections.AddRange(additionalProjections);

        // If this SELECT has GROUP BY and we added correlation projections,
        // we need to also add those columns to the GROUP BY clause
        var newGroupBy = nestedSelect.GroupBy.ToList();
        if (nestedSelect.GroupBy.Count > 0 && additionalProjections.Count > 0)
        {
            foreach (var addedProj in additionalProjections)
            {
                if (addedProj.Expression is ColumnExpression col)
                {
                    // Check if not already in GROUP BY
                    var alreadyInGroupBy = newGroupBy.Any(g =>
                        g is ColumnExpression gc &&
                        gc.TableAlias == col.TableAlias &&
                        gc.Name == col.Name);
                    if (!alreadyInGroupBy)
                    {
                        newGroupBy.Add(col);
                    }
                }
            }
        }

        // Create the transformed select
        var transformedSelect = nestedSelect.Update(
            nestedSelect.Tables,
            newPredicate,
            newGroupBy,
            nestedSelect.Having,
            newProjections,
            nestedSelect.Orderings,
            nestedSelect.Offset,
            nestedSelect.Limit);

        return (transformedSelect, correlations);
    }

    private void EnsureColumnProjected(
        SelectExpression select,
        ColumnExpression column,
        List<ProjectionExpression> additionalProjections,
        Dictionary<ColumnExpression, ColumnExpression> projectionMapping)
    {
        if (projectionMapping.ContainsKey(column))
        {
            return; // Already handled
        }

        // Verify the column is NOT from an outer table
        // If it's from an outer table, we shouldn't try to project it from this SELECT
        if (_outerTableAliases.Contains(column.TableAlias))
        {
            // Column is from an outer table, not from this SELECT's tables
            // Don't add it as a projection - this would create invalid SQL
            return;
        }

        // Also verify the column's table is actually in this SELECT's FROM clause
        // This prevents adding projections for columns from nested subqueries
        var selectTableAliases = CollectSelectTableAliases(select);
        if (!selectTableAliases.Contains(column.TableAlias))
        {
            // Column is from a nested subquery or other scope, not from this SELECT
            return;
        }

        // Check if column is already projected
        foreach (var projection in select.Projection)
        {
            if (projection.Expression is ColumnExpression projCol &&
                projCol.TableAlias == column.TableAlias &&
                projCol.Name == column.Name)
            {
                // Already projected - create reference to it
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
        // Find the projection that exposes this column
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

        // Return as-is if not found (shouldn't happen if properly projected)
        return column;
    }

    private SqlExpression RemapPredicateColumns(
        SqlExpression predicate,
        SelectExpression select,
        Dictionary<ColumnExpression, ColumnExpression> projectionMapping)
    {
        return (SqlExpression)new ColumnRemapper(select, projectionMapping, _outerTableAliases).Visit(predicate);
    }

    private (List<(ColumnExpression outer, SqlExpression inner)> correlatedEqualities,
             List<SqlExpression> nonCorrelated,
             List<SqlExpression> complexCorrelated) SplitPredicate(SqlExpression predicate)
    {
        var correlatedEqualities = new List<(ColumnExpression, SqlExpression)>();
        var nonCorrelated = new List<SqlExpression>();
        var complexCorrelated = new List<SqlExpression>();

        SplitPredicateRecursive(predicate, correlatedEqualities, nonCorrelated, complexCorrelated);

        return (correlatedEqualities, nonCorrelated, complexCorrelated);
    }

    private void SplitPredicateRecursive(
        SqlExpression predicate,
        List<(ColumnExpression, SqlExpression)> correlatedEqualities,
        List<SqlExpression> nonCorrelated,
        List<SqlExpression> complexCorrelated)
    {
        if (predicate is SqlBinaryExpression { OperatorType: ExpressionType.AndAlso } andExpr)
        {
            SplitPredicateRecursive(andExpr.Left, correlatedEqualities, nonCorrelated, complexCorrelated);
            SplitPredicateRecursive(andExpr.Right, correlatedEqualities, nonCorrelated, complexCorrelated);
            return;
        }

        // Check if predicate references outer tables
        var outerColumnFinder = new OuterColumnFinder(_outerTableAliases);
        outerColumnFinder.Visit(predicate);

        if (outerColumnFinder.OuterColumns.Count == 0)
        {
            // No outer references - it's non-correlated
            nonCorrelated.Add(predicate);
            return;
        }

        // Has outer references - check if it's a simple equality
        if (predicate is SqlBinaryExpression { OperatorType: ExpressionType.Equal } eqExpr)
        {
            // Check if one side is an outer column and the other is an inner expression
            if (eqExpr.Left is ColumnExpression leftCol && _outerTableAliases.Contains(leftCol.TableAlias))
            {
                // Left is outer, right is inner
                if (!ContainsOuterReference(eqExpr.Right))
                {
                    correlatedEqualities.Add((leftCol, eqExpr.Right));
                    return;
                }
            }

            if (eqExpr.Right is ColumnExpression rightCol && _outerTableAliases.Contains(rightCol.TableAlias))
            {
                // Right is outer, left is inner
                if (!ContainsOuterReference(eqExpr.Left))
                {
                    correlatedEqualities.Add((rightCol, eqExpr.Left));
                    return;
                }
            }
        }

        // Complex correlated predicate (not simple equality, or both sides reference outer)
        complexCorrelated.Add(predicate);
    }

    private bool ContainsOuterReference(SqlExpression expression)
    {
        var finder = new OuterColumnFinder(_outerTableAliases);
        finder.Visit(expression);
        return finder.OuterColumns.Count > 0;
    }

    private List<ColumnExpression> FindInnerColumns(SqlExpression expression, SelectExpression innerSelect)
    {
        var finder = new InnerColumnFinder(_outerTableAliases);
        finder.Visit(expression);
        return finder.InnerColumns;
    }

    private int GetPredicateCount(SqlExpression predicate)
    {
        if (predicate is SqlBinaryExpression { OperatorType: ExpressionType.AndAlso } andExpr)
        {
            return GetPredicateCount(andExpr.Left) + GetPredicateCount(andExpr.Right);
        }
        return 1;
    }

    private record ApplyTransformResult(SelectExpression TransformedSelect, SqlExpression? JoinPredicate);

    /// <summary>
    /// Checks if a SelectExpression contains any APPLY expressions (directly or nested).
    /// Used for early-out optimization to avoid processing queries that don't need transformation.
    /// </summary>
    private static bool ContainsApplyExpression(SelectExpression select)
    {
        foreach (var table in select.Tables)
        {
            if (ContainsApplyExpressionInTable(table))
            {
                return true;
            }
        }
        return false;
    }

    private static bool ContainsApplyExpressionInTable(TableExpressionBase table)
    {
        switch (table)
        {
            case OuterApplyExpression:
            case CrossApplyExpression:
                return true;

            case JoinExpressionBase join:
                return ContainsApplyExpressionInTable(join.Table);

            case SelectExpression nested:
                return ContainsApplyExpression(nested);

            default:
                return false;
        }
    }

    /// <summary>
    /// Collects all table aliases from a SELECT's FROM clause (non-recursive, only direct tables).
    /// </summary>
    private static HashSet<string> CollectSelectTableAliases(SelectExpression select)
    {
        var aliases = new HashSet<string>();
        foreach (var table in select.Tables)
        {
            CollectTableAliasesFromTable(table, aliases);
        }
        return aliases;
    }

    private static void CollectTableAliasesFromTable(TableExpressionBase table, HashSet<string> aliases)
    {
        // Get the base table (unwrap joins)
        var baseTable = table.UnwrapJoin();

        if (baseTable.Alias != null)
        {
            aliases.Add(baseTable.Alias);
        }

        // For join expressions, also get the joined table
        if (table is JoinExpressionBase join)
        {
            CollectTableAliasesFromTable(join.Table, aliases);
        }
    }

    /// <summary>
    /// Finds column references to outer tables.
    /// </summary>
    private class OuterColumnFinder : ExpressionVisitor
    {
        private readonly HashSet<string> _outerTableAliases;

        public HashSet<ColumnExpression> OuterColumns { get; } = new();

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

    /// <summary>
    /// Finds column references to inner tables (non-outer).
    /// </summary>
    private class InnerColumnFinder : ExpressionVisitor
    {
        private readonly HashSet<string> _outerTableAliases;

        public List<ColumnExpression> InnerColumns { get; } = new();

        public InnerColumnFinder(HashSet<string> outerTableAliases)
        {
            _outerTableAliases = outerTableAliases;
        }

        protected override Expression VisitExtension(Expression node)
        {
            if (node is ColumnExpression column && !_outerTableAliases.Contains(column.TableAlias))
            {
                InnerColumns.Add(column);
            }

            return base.VisitExtension(node);
        }
    }

    /// <summary>
    /// Remaps column references from inner table aliases to the select's projected columns.
    /// </summary>
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
                // Don't remap outer columns
                if (_outerTableAliases.Contains(column.TableAlias))
                {
                    return column;
                }

                // Check if we have a mapping for this column
                if (_projectionMapping.TryGetValue(column, out var mappedColumn))
                {
                    return mappedColumn;
                }

                // Try to find in projections
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

    /// <summary>
    /// Remaps column references from join table aliases to outer table columns.
    /// Used when APPLY expressions have correlated projections that need to be
    /// resolved from the outer table instead of through the join.
    /// </summary>
    private class CorrelatedColumnRemapper : ExpressionVisitor
    {
        private readonly Dictionary<(string tableAlias, string columnName), ColumnExpression> _remappings;

        public CorrelatedColumnRemapper(Dictionary<(string tableAlias, string columnName), ColumnExpression> remappings)
        {
            _remappings = remappings;
        }

        protected override Expression VisitExtension(Expression node)
        {
            if (node is ColumnExpression column)
            {
                var key = (column.TableAlias, column.Name);
                if (_remappings.TryGetValue(key, out var remappedColumn))
                {
                    return remappedColumn;
                }
            }

            return base.VisitExtension(node);
        }
    }
}