using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using System.Linq.Expressions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal;

/// <summary>
/// BigQuery treats backtick-quoted qualified references like `c`.`Region` as struct field access
/// when `c` is also a column alias in the same SELECT scope. This postprocessor renames projection
/// aliases that conflict with table aliases within the same subquery, and updates all outer
/// column references accordingly.
/// </summary>
public class BigQueryColumnAliasPostprocessor : ExpressionVisitor
{
    protected override Expression VisitExtension(Expression node)
    {
        if (node is SelectExpression select)
        {
            return VisitSelect(select);
        }

        if (node is ShapedQueryExpression shaped)
        {
            return shaped
                .UpdateQueryExpression(Visit(shaped.QueryExpression))
                .UpdateShaperExpression(Visit(shaped.ShaperExpression));
        }

        return base.VisitExtension(node);
    }

    private Expression VisitSelect(SelectExpression select)
    {
        // Process tables recursively and collect renames for subqueries that have conflicts
        var processedTables = new List<TableExpressionBase>();
        // Maps subquery alias -> (oldColumnName -> newColumnName)
        var renames = new Dictionary<string, Dictionary<string, string>>();
        var tablesChanged = false;

        foreach (var table in select.Tables)
        {
            var (processed, changed) = ProcessTable(table, renames);
            processedTables.Add(processed);
            if (changed)
            {
                tablesChanged = true;
            }
        }

        if (!tablesChanged && renames.Count == 0)
        {
            return select;
        }

        if (renames.Count == 0)
        {
            return select.Update(
                processedTables,
                select.Predicate,
                select.GroupBy,
                select.Having,
                select.Projection,
                select.Orderings,
                select.Offset,
                select.Limit);
        }

        // Update column references in the outer query to use renamed aliases
        var newProjections = new List<ProjectionExpression>();
        var projectionsChanged = false;

        foreach (var projection in select.Projection)
        {
            var newExpression = RenameColumns(projection.Expression, renames);
            if (newExpression != projection.Expression)
            {
                newProjections.Add(new ProjectionExpression(newExpression, projection.Alias));
                projectionsChanged = true;
            }
            else
            {
                newProjections.Add(projection);
            }
        }

        var newPredicate = select.Predicate != null
            ? RenameColumns(select.Predicate, renames)
            : null;
        var predicateChanged = newPredicate != select.Predicate;

        var newHaving = select.Having != null
            ? RenameColumns(select.Having, renames)
            : null;
        var havingChanged = newHaving != select.Having;

        var newOrderings = new List<OrderingExpression>();
        var orderingsChanged = false;

        foreach (var ordering in select.Orderings)
        {
            var newExpression = RenameColumns(ordering.Expression, renames);
            if (newExpression != ordering.Expression)
            {
                newOrderings.Add(new OrderingExpression(newExpression, ordering.IsAscending));
                orderingsChanged = true;
            }
            else
            {
                newOrderings.Add(ordering);
            }
        }

        return select.Update(
            processedTables,
            predicateChanged ? newPredicate : select.Predicate,
            select.GroupBy,
            havingChanged ? newHaving : select.Having,
            projectionsChanged ? newProjections : select.Projection,
            orderingsChanged ? newOrderings : select.Orderings,
            select.Offset,
            select.Limit);
    }

    private (TableExpressionBase table, bool changed) ProcessTable(
        TableExpressionBase table,
        Dictionary<string, Dictionary<string, string>> renames)
    {
        if (table is LeftJoinExpression leftJoin)
        {
            var (processed, changed) = ProcessTable(leftJoin.Table, renames);
            if (changed)
            {
                return (new LeftJoinExpression(processed, leftJoin.JoinPredicate, leftJoin.IsPrunable), true);
            }
            return (table, false);
        }

        if (table is InnerJoinExpression innerJoin)
        {
            var (processed, changed) = ProcessTable(innerJoin.Table, renames);
            if (changed)
            {
                return (new InnerJoinExpression(processed, innerJoin.JoinPredicate, innerJoin.IsPrunable), true);
            }
            return (table, false);
        }

        if (table is CrossJoinExpression crossJoin)
        {
            var (processed, changed) = ProcessTable(crossJoin.Table, renames);
            if (changed)
            {
                return (new CrossJoinExpression(processed), true);
            }
            return (table, false);
        }

        if (table is SelectExpression subquery)
        {
            // Recursively process the subquery first
            var processed = (SelectExpression)VisitSelect(subquery);

            // Check for conflicting projection aliases within the processed subquery
            var tableAliases = CollectTableAliases(processed);
            var conflicting = new Dictionary<string, string>();

            var existingAliases = new HashSet<string>(
                processed.Projection.Select(p => p.Alias),
                StringComparer.OrdinalIgnoreCase);

            foreach (var proj in processed.Projection)
            {
                if (tableAliases.Contains(proj.Alias))
                {
                    var newAlias = GenerateUniqueAlias(proj.Alias, existingAliases, tableAliases);
                    conflicting[proj.Alias] = newAlias;
                    existingAliases.Add(newAlias);
                }
            }

            if (conflicting.Count > 0)
            {
                // Create new projections with renamed aliases
                var newProjections = processed.Projection
                    .Select(p => conflicting.TryGetValue(p.Alias, out var newAlias)
                        ? new ProjectionExpression(p.Expression, newAlias)
                        : p)
                    .ToList();

                processed = processed.Update(
                    processed.Tables,
                    processed.Predicate,
                    processed.GroupBy,
                    processed.Having,
                    newProjections,
                    processed.Orderings,
                    processed.Offset,
                    processed.Limit);

                // Track renames so outer query column references can be updated
                if (subquery.Alias != null)
                {
                    renames[subquery.Alias] = conflicting;
                }
            }

            if (processed != subquery)
            {
                return (processed, true);
            }
        }

        return (table, false);
    }

    private static HashSet<string> CollectTableAliases(SelectExpression select)
    {
        var aliases = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var table in select.Tables)
        {
            CollectTableAliasesRecursive(table, aliases);
        }

        return aliases;
    }

    private static void CollectTableAliasesRecursive(TableExpressionBase table, HashSet<string> aliases)
    {
        var unwrapped = table.UnwrapJoin();

        if (unwrapped.Alias != null && unwrapped is not SelectExpression)
        {
            aliases.Add(unwrapped.Alias);
        }

        if (table is LeftJoinExpression leftJoin)
        {
            CollectTableAliasesRecursive(leftJoin.Table, aliases);
        }
        else if (table is InnerJoinExpression innerJoin)
        {
            CollectTableAliasesRecursive(innerJoin.Table, aliases);
        }
        else if (table is CrossJoinExpression crossJoin)
        {
            CollectTableAliasesRecursive(crossJoin.Table, aliases);
        }
    }

    private static string GenerateUniqueAlias(
        string baseAlias,
        HashSet<string> existingAliases,
        HashSet<string> tableAliases)
    {
        var counter = 0;
        string candidate;

        do
        {
            candidate = $"{baseAlias}{counter}";
            counter++;
        }
        while (existingAliases.Contains(candidate) || tableAliases.Contains(candidate));

        return candidate;
    }

    private static SqlExpression RenameColumns(
        SqlExpression expression,
        Dictionary<string, Dictionary<string, string>> renames)
    {
        if (expression is ColumnExpression column
            && renames.TryGetValue(column.TableAlias, out var colRenames)
            && colRenames.TryGetValue(column.Name, out var newName))
        {
            return new ColumnExpression(
                newName,
                column.TableAlias,
                column.Type,
                column.TypeMapping,
                column.IsNullable);
        }

        if (expression is SqlBinaryExpression binary)
        {
            var newLeft = RenameColumns(binary.Left, renames);
            var newRight = RenameColumns(binary.Right, renames);
            if (newLeft != binary.Left || newRight != binary.Right)
            {
                return new SqlBinaryExpression(
                    binary.OperatorType,
                    newLeft,
                    newRight,
                    binary.Type,
                    binary.TypeMapping);
            }
        }

        if (expression is SqlUnaryExpression unary)
        {
            var newOperand = RenameColumns(unary.Operand, renames);
            if (newOperand != unary.Operand)
            {
                return new SqlUnaryExpression(
                    unary.OperatorType,
                    newOperand,
                    unary.Type,
                    unary.TypeMapping);
            }
        }

        if (expression is SqlFunctionExpression function && function.Arguments != null)
        {
            var newArgs = new SqlExpression[function.Arguments.Count];
            var changed = false;

            for (var i = 0; i < function.Arguments.Count; i++)
            {
                newArgs[i] = RenameColumns(function.Arguments[i], renames);
                if (newArgs[i] != function.Arguments[i])
                {
                    changed = true;
                }
            }

            if (changed)
            {
                return new SqlFunctionExpression(
                    function.Name,
                    newArgs,
                    function.IsNullable,
                    function.ArgumentsPropagateNullability!,
                    function.Type,
                    function.TypeMapping);
            }
        }

        if (expression is CaseExpression caseExpr)
        {
            var newWhenClauses = new List<CaseWhenClause>();
            var changed = false;

            foreach (var whenClause in caseExpr.WhenClauses)
            {
                var newTest = RenameColumns(whenClause.Test, renames);
                var newResult = RenameColumns(whenClause.Result, renames);
                if (newTest != whenClause.Test || newResult != whenClause.Result)
                {
                    changed = true;
                }
                newWhenClauses.Add(new CaseWhenClause(newTest, newResult));
            }

            SqlExpression? newElse = null;
            if (caseExpr.ElseResult != null)
            {
                newElse = RenameColumns(caseExpr.ElseResult, renames);
                if (newElse != caseExpr.ElseResult)
                {
                    changed = true;
                }
            }

            if (changed)
            {
                return caseExpr.Operand != null
                    ? new CaseExpression(caseExpr.Operand, newWhenClauses, newElse)
                    : new CaseExpression(newWhenClauses, newElse);
            }
        }

        return expression;
    }
}
