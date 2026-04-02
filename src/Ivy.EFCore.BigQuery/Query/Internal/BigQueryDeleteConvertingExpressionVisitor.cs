using System.Linq.Expressions;
using Ivy.EntityFrameworkCore.BigQuery.Query.Expressions.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal;

/// <summary>
/// Transforms EF Core's <see cref="DeleteExpression"/> into <see cref="BigQueryDeleteExpression"/>
/// to support BigQuery's DELETE syntax with USING clause for multi-table scenarios.
/// </summary>
/// <remarks>
/// <para>
/// BigQuery DELETE syntax: <c>DELETE FROM table USING other_tables WHERE condition</c>
/// </para>
/// <para>
/// This transformation is necessary because EF Core's standard DeleteExpression embeds
/// joined tables within a SelectExpression, but BigQuery requires them in a separate USING clause.
/// </para>
/// </remarks>
public class BigQueryDeleteConvertingExpressionVisitor : ExpressionVisitor
{
    /// <summary>
    /// Transforms a <see cref="DeleteExpression"/> into a <see cref="BigQueryDeleteExpression"/>.
    /// Non-delete expressions pass through unchanged.
    /// </summary>
    public virtual Expression Process(Expression node)
        => node is DeleteExpression deleteExpression
            ? TransformToUsingClauseSyntax(deleteExpression)
            : node;

    private BigQueryDeleteExpression TransformToUsingClauseSyntax(DeleteExpression deleteExpression)
    {
        var select = deleteExpression.SelectExpression;

        ValidateNoUnsupportedClauses(select);

        var (usingTables, combinedPredicate) = ExtractJoinedTablesAndPredicates(
            select.Tables,
            deleteExpression.Table.Alias,
            select.Predicate);

        return new BigQueryDeleteExpression(
            deleteExpression.Table,
            usingTables,
            combinedPredicate,
            deleteExpression.Tags);
    }

    private static void ValidateNoUnsupportedClauses(SelectExpression select)
    {
        // BQ's DELETE doesn't support these clauses
        if (select is not { Offset: null, Limit: null, Having: null }
            || select.Orderings.Count > 0
            || select.GroupBy.Count > 0
            || select.Projection.Count > 0)
        {
            throw new InvalidOperationException(
                RelationalStrings.ExecuteOperationWithUnsupportedOperatorInSqlGeneration(
                    nameof(EntityFrameworkQueryableExtensions.ExecuteDelete)));
        }
    }

    private static (List<TableExpressionBase> UsingTables, SqlExpression? Predicate) ExtractJoinedTablesAndPredicates(
        IReadOnlyList<TableExpressionBase> tables,
        string targetTableAlias,
        SqlExpression? wherePredicate)
    {
        var usingTables = new List<TableExpressionBase>();
        SqlExpression? accumulatedJoinConditions = null;

        foreach (var table in tables)
        {
            var (tableToAdd, joinCondition) = table switch
            {
                // Plain table reference - add to USING if not the target
                TableExpression t => (t.Alias != targetTableAlias ? t : null, (SqlExpression?)null),

                // Inner join - extract the table and accumulate the join condition
                InnerJoinExpression { Table: var t, JoinPredicate: var p }
                    => (t.Alias != targetTableAlias ? t : null, p),

                _ => throw new InvalidOperationException(
                    RelationalStrings.ExecuteOperationWithUnsupportedOperatorInSqlGeneration(
                        nameof(EntityFrameworkQueryableExtensions.ExecuteDelete)))
            };

            if (tableToAdd is not null)
            {
                usingTables.Add(tableToAdd);
            }

            if (joinCondition is not null)
            {
                accumulatedJoinConditions = CombinePredicates(accumulatedJoinConditions, joinCondition);
            }
        }


        var finalPredicate = CombinePredicates(accumulatedJoinConditions, wherePredicate);

        return (usingTables, finalPredicate);
    }

    /// <summary>
    /// Merge join conditions with the original WHERE clause
    /// </summary>
    private static SqlExpression? CombinePredicates(SqlExpression? left, SqlExpression? right)
        => (left, right) switch
        {
            (null, _) => right,
            (_, null) => left,
            _ => new SqlBinaryExpression(
                ExpressionType.AndAlso,
                left,
                right,
                typeof(bool),
                left.TypeMapping)
        };
}
