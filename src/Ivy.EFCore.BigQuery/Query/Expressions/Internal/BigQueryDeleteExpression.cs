using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Expressions.Internal;

/// <summary>
/// An SQL expression that represents a BigQuery DELETE operation with USING clause support.
/// </summary>
public sealed class BigQueryDeleteExpression : Expression, IPrintableExpression
{
    /// <summary>
    /// The table on which the delete operation is being applied.
    /// </summary>
    public TableExpression Table { get; }

    /// <summary>
    /// Additional tables which can be referenced in the predicate (via USING clause).
    /// </summary>
    public IReadOnlyList<TableExpressionBase> FromItems { get; }

    /// <summary>
    /// The WHERE predicate for the DELETE.
    /// </summary>
    public SqlExpression? Predicate { get; }

    /// <summary>
    /// The list of tags applied to this <see cref="BigQueryDeleteExpression" />.
    /// </summary>
    public ISet<string> Tags { get; }

    /// <summary>
    /// Creates a new instance of the <see cref="BigQueryDeleteExpression" /> class.    
    /// </summary>
    /// <param name="table">The table on which the delete operation is being applied.</param>
    /// <param name="fromItems">Additional tables which can be referenced in the predicate (via USING clause).</param>
    /// <param name="predicate">The WHERE predicate for the DELETE.</param>
    /// <param name="tags">The list of tags applied to this <see cref="BigQueryDeleteExpression" />.</param>
    public BigQueryDeleteExpression(
        TableExpression table,
        IReadOnlyList<TableExpressionBase> fromItems,
        SqlExpression? predicate,
        ISet<string> tags)
    {
        Table = table;
        FromItems = fromItems;
        Predicate = predicate;
        Tags = tags;
    }

    /// <inheritdoc />
    public override ExpressionType NodeType => ExpressionType.Extension;

    /// <inheritdoc />
    public override Type Type => typeof(object);

    /// <inheritdoc />
    protected override Expression VisitChildren(ExpressionVisitor visitor)
        => Predicate is null
            ? this
            : Update((SqlExpression?)visitor.Visit(Predicate));

    /// <summary>
    /// Creates a new expression that is like this one, but using the supplied children.
    /// If all of the children are the same, it will return this expression.
    /// </summary>
    /// <param name="predicate">The <see cref="Predicate" /> property of the result.</param>
    // <returns>This expression if no children changed, or an expression with the updated children.</returns>
    public BigQueryDeleteExpression Update(SqlExpression? predicate)
        => predicate == Predicate
            ? this
            : new BigQueryDeleteExpression(Table, FromItems, predicate, Tags);

    /// <inheritdoc />
    public void Print(ExpressionPrinter printer)
    {
        printer.AppendLine($"DELETE FROM {Table.Name} AS {Table.Alias}");

        if (FromItems.Count > 0)
        {
            var first = true;
            foreach (var fromItem in FromItems)
            {
                if (first)
                {
                    printer.Append("USING ");
                    first = false;
                }
                else
                {
                    printer.Append(", ");
                }

                printer.Visit(fromItem);
            }
        }

        if (Predicate is not null)
        {
            printer.Append("WHERE ");
            printer.Visit(Predicate);
        }
    }

    private bool Equals(BigQueryDeleteExpression other)
    => Table == other.Table
        && FromItems.SequenceEqual(other.FromItems)
        && (Predicate is null ? other.Predicate is null : Predicate.Equals(other.Predicate));

    /// <inheritdoc />
    public override bool Equals(object? obj)
        => obj != null
            && (ReferenceEquals(this, obj)
                || obj is BigQueryDeleteExpression bqDeleteExpression
                && Equals(bqDeleteExpression));

    /// <inheritdoc />
    public override int GetHashCode()
        => Table.GetHashCode();
}