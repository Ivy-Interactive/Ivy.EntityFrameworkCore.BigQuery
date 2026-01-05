using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
using System.Linq.Expressions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal;

#pragma warning disable EF9100 // RelationalExpressionQuotingUtilities is for evaluation purposes
/// <summary>
/// Represents a BigQuery IN UNNEST expression: item IN UNNEST(array)
/// </summary>
public class BigQueryInUnnestExpression : SqlExpression
{
    public BigQueryInUnnestExpression(
        SqlExpression item,
        SqlExpression array,
        RelationalTypeMapping? typeMapping)
        : base(typeof(bool), typeMapping)
    {
        Item = item;
        Array = array;
    }

    public virtual SqlExpression Item { get; }
    public virtual SqlExpression Array { get; }

    protected override Expression VisitChildren(ExpressionVisitor visitor)
    {
        var visitedItem = (SqlExpression)visitor.Visit(Item);
        var visitedArray = (SqlExpression)visitor.Visit(Array);

        return Update(visitedItem, visitedArray);
    }

    public virtual BigQueryInUnnestExpression Update(SqlExpression item, SqlExpression array)
        => item != Item || array != Array
            ? new BigQueryInUnnestExpression(item, array, TypeMapping)
            : this;

    protected override void Print(ExpressionPrinter expressionPrinter)
    {
        expressionPrinter.Visit(Item);
        expressionPrinter.Append(" IN UNNEST(");
        expressionPrinter.Visit(Array);
        expressionPrinter.Append(")");
    }

    /// <inheritdoc />
    public override Expression Quote()
        => New(
            typeof(BigQueryInUnnestExpression).GetConstructor(
                [typeof(SqlExpression), typeof(SqlExpression), typeof(RelationalTypeMapping)])!,
            Item.Quote(),
            Array.Quote(),
            RelationalExpressionQuotingUtilities.QuoteTypeMapping(TypeMapping));

    public override bool Equals(object? obj)
        => obj is BigQueryInUnnestExpression other
            && Item.Equals(other.Item)
            && Array.Equals(other.Array);

    public override int GetHashCode()
        => HashCode.Combine(Item, Array);
}
