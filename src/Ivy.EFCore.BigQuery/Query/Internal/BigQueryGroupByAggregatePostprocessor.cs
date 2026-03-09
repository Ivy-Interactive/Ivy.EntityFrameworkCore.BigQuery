using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
using System.Linq.Expressions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal;

/// <summary>
/// BigQuery requires all SELECT columns to be either in GROUP BY or aggregated.
/// This postprocessor wraps columns from joined subqueries (like those created by
/// the correlated subquery postprocessor) with ANY_VALUE() when the outer query has GROUP BY.
/// </summary>
public class BigQueryGroupByAggregatePostprocessor : ExpressionVisitor
{
    private readonly ISqlExpressionFactory _sqlExpressionFactory;

    public BigQueryGroupByAggregatePostprocessor(ISqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    protected override Expression VisitExtension(Expression node)
    {
        if (node is SelectExpression select)
        {
            return VisitSelect(select);
        }

        if (node is ShapedQueryExpression shapedQueryExpression)
        {
            return shapedQueryExpression
                .UpdateQueryExpression(Visit(shapedQueryExpression.QueryExpression))
                .UpdateShaperExpression(Visit(shapedQueryExpression.ShaperExpression));
        }

        if (node is ScalarSubqueryExpression scalarSubquery)
        {
            var visitedSubquery = (SelectExpression)VisitSelect(scalarSubquery.Subquery);
            if (visitedSubquery != scalarSubquery.Subquery)
            {
                return new ScalarSubqueryExpression(visitedSubquery);
            }
            return node;
        }

        return base.VisitExtension(node);
    }

    private Expression VisitSelect(SelectExpression select)
    {
        var processedTables = new List<TableExpressionBase>();
        var tablesChanged = false;

        foreach (var table in select.Tables)
        {
            var processed = ProcessTable(table);
            processedTables.Add(processed);
            if (processed != table)
            {
                tablesChanged = true;
            }
        }

        var newPredicate = select.Predicate != null ? VisitSqlExpression(select.Predicate) : null;
        var predicateChanged = newPredicate != select.Predicate;

        var newHaving = select.Having != null ? VisitSqlExpression(select.Having) : null;
        var havingChanged = newHaving != select.Having;

        if (select.GroupBy.Count == 0)
        {
            if (tablesChanged || predicateChanged || havingChanged)
            {
                return select.Update(
                    processedTables,
                    newPredicate,
                    select.GroupBy,
                    newHaving,
                    select.Projection,
                    select.Orderings,
                    select.Offset,
                    select.Limit);
            }
            return select;
        }


        var baseTables = CollectBaseTableAliases(select);

        var groupByColumns = CollectGroupByColumns(select.GroupBy);

        var newProjections = new List<ProjectionExpression>();
        var projectionsChanged = false;

        foreach (var projection in select.Projection)
        {
            var newExpression = WrapNonGroupedColumns(
                projection.Expression,
                groupByColumns,
                baseTables);

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

        if (tablesChanged || projectionsChanged || predicateChanged || havingChanged)
        {
            return select.Update(
                processedTables,
                newPredicate,
                select.GroupBy,
                newHaving,
                projectionsChanged ? newProjections : select.Projection,
                select.Orderings,
                select.Offset,
                select.Limit);
        }

        return select;
    }

    /// <summary>
    /// Visits a SQL expression to process any nested scalar subqueries.
    /// </summary>
    private SqlExpression VisitSqlExpression(SqlExpression expression)
    {
        if (expression is ScalarSubqueryExpression scalarSubquery)
        {
            var visitedSubquery = (SelectExpression)VisitSelect(scalarSubquery.Subquery);
            if (visitedSubquery != scalarSubquery.Subquery)
            {
                return new ScalarSubqueryExpression(visitedSubquery);
            }
            return expression;
        }

        if (expression is SqlBinaryExpression binary)
        {
            var newLeft = VisitSqlExpression(binary.Left);
            var newRight = VisitSqlExpression(binary.Right);
            if (newLeft != binary.Left || newRight != binary.Right)
            {
                return _sqlExpressionFactory.MakeBinary(
                    binary.OperatorType,
                    newLeft,
                    newRight,
                    binary.TypeMapping)!;
            }
        }

        if (expression is SqlUnaryExpression unary)
        {
            var newOperand = VisitSqlExpression(unary.Operand);
            if (newOperand != unary.Operand)
            {
                return _sqlExpressionFactory.MakeUnary(
                    unary.OperatorType,
                    newOperand,
                    unary.Type,
                    unary.TypeMapping)!;
            }
        }

        if (expression is InExpression inExpr)
        {
            if (inExpr.Subquery != null)
            {
                var visitedSubquery = (SelectExpression)VisitSelect(inExpr.Subquery);
                if (visitedSubquery != inExpr.Subquery)
                {
                    return new InExpression(
                        inExpr.Item,
                        visitedSubquery,
                        inExpr.TypeMapping);
                }
            }
        }

        if (expression is ExistsExpression existsExpr)
        {
            var visitedSubquery = (SelectExpression)VisitSelect(existsExpr.Subquery);
            if (visitedSubquery != existsExpr.Subquery)
            {
                return new ExistsExpression(visitedSubquery, existsExpr.TypeMapping);
            }
        }

        if (expression is SqlFunctionExpression function && function.Arguments != null)
        {
            var newArgs = new SqlExpression[function.Arguments.Count];
            var changed = false;
            for (var i = 0; i < function.Arguments.Count; i++)
            {
                newArgs[i] = VisitSqlExpression(function.Arguments[i]);
                if (newArgs[i] != function.Arguments[i])
                {
                    changed = true;
                }
            }
            if (changed)
            {
                return _sqlExpressionFactory.Function(
                    function.Name,
                    newArgs,
                    function.IsNullable,
                    function.ArgumentsPropagateNullability ?? Enumerable.Repeat(true, newArgs.Length),
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
                var newTest = VisitSqlExpression(whenClause.Test);
                var newResult = VisitSqlExpression(whenClause.Result);
                if (newTest != whenClause.Test || newResult != whenClause.Result)
                {
                    changed = true;
                }
                newWhenClauses.Add(new CaseWhenClause(newTest, newResult));
            }
            SqlExpression? newElse = null;
            if (caseExpr.ElseResult != null)
            {
                newElse = VisitSqlExpression(caseExpr.ElseResult);
                if (newElse != caseExpr.ElseResult)
                {
                    changed = true;
                }
            }
            if (changed)
            {
                return _sqlExpressionFactory.Case(newWhenClauses, newElse);
            }
        }

        return expression;
    }

    private TableExpressionBase ProcessTable(TableExpressionBase table)
    {
        return table switch
        {
            LeftJoinExpression leftJoin => ProcessLeftJoin(leftJoin),
            InnerJoinExpression innerJoin => ProcessInnerJoin(innerJoin),
            CrossJoinExpression crossJoin => ProcessCrossJoin(crossJoin),
            SelectExpression nestedSelect => (TableExpressionBase)VisitSelect(nestedSelect),
            _ => table
        };
    }

    private LeftJoinExpression ProcessLeftJoin(LeftJoinExpression leftJoin)
    {
        var processedTable = ProcessTable(leftJoin.Table);
        if (processedTable != leftJoin.Table)
        {
            return new LeftJoinExpression(processedTable, leftJoin.JoinPredicate, leftJoin.IsPrunable);
        }
        return leftJoin;
    }

    private InnerJoinExpression ProcessInnerJoin(InnerJoinExpression innerJoin)
    {
        var processedTable = ProcessTable(innerJoin.Table);
        if (processedTable != innerJoin.Table)
        {
            return new InnerJoinExpression(processedTable, innerJoin.JoinPredicate, innerJoin.IsPrunable);
        }
        return innerJoin;
    }

    private CrossJoinExpression ProcessCrossJoin(CrossJoinExpression crossJoin)
    {
        var processedTable = ProcessTable(crossJoin.Table);
        if (processedTable != crossJoin.Table)
        {
            return new CrossJoinExpression(processedTable);
        }
        return crossJoin;
    }

    private HashSet<string> CollectBaseTableAliases(SelectExpression select)
    {
        var baseTableAliases = new HashSet<string>();

        foreach (var table in select.Tables)
        {
            CollectBaseTableAliasesRecursive(table, baseTableAliases);
        }

        return baseTableAliases;
    }

    private void CollectBaseTableAliasesRecursive(TableExpressionBase table, HashSet<string> aliases)
    {
        var unwrapped = table.UnwrapJoin();

        if (unwrapped is TableExpression tableExpr)
        {
            //Base table
            if (tableExpr.Alias != null)
            {
                aliases.Add(tableExpr.Alias);
            }
        }
        else if (unwrapped is SelectExpression)
        {
            // Subquery
        }
        else
        {
            // Other table types (like TableValuedFunctionExpression)
            if (unwrapped.Alias != null)
            {
                aliases.Add(unwrapped.Alias);
            }
        }

        if (table is LeftJoinExpression leftJoin)
        {
            CollectBaseTableAliasesRecursive(leftJoin.Table, aliases);
        }
        else if (table is InnerJoinExpression innerJoin)
        {
            CollectBaseTableAliasesRecursive(innerJoin.Table, aliases);
        }
        else if (table is CrossJoinExpression crossJoin)
        {
            CollectBaseTableAliasesRecursive(crossJoin.Table, aliases);
        }
    }

    private HashSet<(string tableAlias, string columnName)> CollectGroupByColumns(IReadOnlyList<SqlExpression> groupBy)
    {
        var columns = new HashSet<(string, string)>();

        foreach (var expr in groupBy)
        {
            CollectColumnsRecursive(expr, columns);
        }

        return columns;
    }

    private void CollectColumnsRecursive(SqlExpression expr, HashSet<(string, string)> columns)
    {
        if (expr is ColumnExpression column)
        {
            columns.Add((column.TableAlias, column.Name));
        }
        else if (expr is SqlBinaryExpression binary)
        {
            CollectColumnsRecursive(binary.Left, columns);
            CollectColumnsRecursive(binary.Right, columns);
        }
        else if (expr is SqlUnaryExpression unary)
        {
            CollectColumnsRecursive(unary.Operand, columns);
        }
        else if (expr is SqlFunctionExpression function && function.Arguments != null)
        {
            foreach (var arg in function.Arguments)
            {
                CollectColumnsRecursive(arg, columns);
            }
        }
    }

    private SqlExpression WrapNonGroupedColumns(
        SqlExpression expression,
        HashSet<(string tableAlias, string columnName)> groupByColumns,
        HashSet<string> baseTables)
    {
        if (expression is ColumnExpression column)
        {
            if (groupByColumns.Contains((column.TableAlias, column.Name)))
            {
                return expression;
            }

            if (baseTables.Contains(column.TableAlias))
            {
                return expression;
            }

            // Column from a joined subquery - wrap with MIN aggregate
            // MIN is used instead of ANY_VALUE for deterministic results:
            // - When there's exactly one value per group (common with ROW_NUMBER rn=1 joins),
            //   MIN returns that value
            // - When there are multiple values, MIN returns the smallest, giving deterministic behavior
            // - This is important for bulk update/delete operations where row counts must be predictable
            return _sqlExpressionFactory.Function(
                "MIN",
                new[] { column },
                nullable: true,
                argumentsPropagateNullability: new[] { true },
                column.Type,
                column.TypeMapping);
        }

        if (expression is SqlBinaryExpression binary)
        {
            var newLeft = WrapNonGroupedColumns(binary.Left, groupByColumns, baseTables);
            var newRight = WrapNonGroupedColumns(binary.Right, groupByColumns, baseTables);

            if (newLeft != binary.Left || newRight != binary.Right)
            {
                return _sqlExpressionFactory.MakeBinary(
                    binary.OperatorType,
                    newLeft,
                    newRight,
                    binary.TypeMapping)!;
            }
        }

        if (expression is SqlUnaryExpression unary)
        {
            var newOperand = WrapNonGroupedColumns(unary.Operand, groupByColumns, baseTables);

            if (newOperand != unary.Operand)
            {
                return _sqlExpressionFactory.MakeUnary(
                    unary.OperatorType,
                    newOperand,
                    unary.Type,
                    unary.TypeMapping)!;
            }
        }

        if (expression is SqlFunctionExpression function && function.Arguments != null)
        {
            if (IsAggregateFunction(function.Name))
            {
                return expression;
            }

            var newArgs = new SqlExpression[function.Arguments.Count];
            var changed = false;

            for (var i = 0; i < function.Arguments.Count; i++)
            {
                var newArg = WrapNonGroupedColumns(function.Arguments[i], groupByColumns, baseTables);
                newArgs[i] = newArg;
                if (newArg != function.Arguments[i])
                {
                    changed = true;
                }
            }

            if (changed)
            {
                return _sqlExpressionFactory.Function(
                    function.Name,
                    newArgs,
                    function.IsNullable,
                    function.ArgumentsPropagateNullability ?? Enumerable.Repeat(true, newArgs.Length),
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
                var newTest = WrapNonGroupedColumns(whenClause.Test, groupByColumns, baseTables);
                var newResult = WrapNonGroupedColumns(whenClause.Result, groupByColumns, baseTables);

                if (newTest != whenClause.Test || newResult != whenClause.Result)
                {
                    changed = true;
                }

                newWhenClauses.Add(new CaseWhenClause(newTest, newResult));
            }

            SqlExpression? newElse = null;
            if (caseExpr.ElseResult != null)
            {
                newElse = WrapNonGroupedColumns(caseExpr.ElseResult, groupByColumns, baseTables);
                if (newElse != caseExpr.ElseResult)
                {
                    changed = true;
                }
            }

            if (changed)
            {
                return _sqlExpressionFactory.Case(
                    newWhenClauses,
                    newElse);
            }
        }

        return expression;
    }

    private static bool IsAggregateFunction(string name)
    {
        return name.Equals("COUNT", StringComparison.OrdinalIgnoreCase) ||
               name.Equals("SUM", StringComparison.OrdinalIgnoreCase) ||
               name.Equals("AVG", StringComparison.OrdinalIgnoreCase) ||
               name.Equals("MIN", StringComparison.OrdinalIgnoreCase) ||
               name.Equals("MAX", StringComparison.OrdinalIgnoreCase) ||
               name.Equals("COUNT_BIG", StringComparison.OrdinalIgnoreCase) ||
               name.Equals("STRING_AGG", StringComparison.OrdinalIgnoreCase) ||
               name.Equals("ARRAY_AGG", StringComparison.OrdinalIgnoreCase) ||
               name.Equals("ANY_VALUE", StringComparison.OrdinalIgnoreCase) ||
               name.Equals("BIT_AND", StringComparison.OrdinalIgnoreCase) ||
               name.Equals("BIT_OR", StringComparison.OrdinalIgnoreCase) ||
               name.Equals("BIT_XOR", StringComparison.OrdinalIgnoreCase) ||
               name.Equals("COUNTIF", StringComparison.OrdinalIgnoreCase) ||
               name.Equals("LOGICAL_AND", StringComparison.OrdinalIgnoreCase) ||
               name.Equals("LOGICAL_OR", StringComparison.OrdinalIgnoreCase);
    }
}