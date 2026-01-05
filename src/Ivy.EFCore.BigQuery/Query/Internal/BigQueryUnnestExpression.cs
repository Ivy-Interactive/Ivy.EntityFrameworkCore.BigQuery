using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
using System.Linq.Expressions;

#pragma warning disable EF9100 // RelationalExpressionQuotingUtilities is for evaluation purposes

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal;

/// <summary>
/// Represents BigQuery UNNEST table expression: UNNEST(array) AS alias
/// </summary>
public class BigQueryUnnestExpression : TableExpressionBase
{
    /// <summary>
    /// The array expression being unnested
    /// </summary>
    public virtual SqlExpression Array { get; }

    /// <summary>
    /// The CLR element type
    /// </summary>
    public virtual Type ElementType { get; }

    /// <summary>
    /// The type mapping for array elements
    /// </summary>
    public virtual RelationalTypeMapping? ElementTypeMapping { get; }

    /// <summary>
    /// Whether to include WITH OFFSET clause
    /// </summary>
    public virtual bool WithOffset { get; }

    /// <summary>
    /// Alias for the offset column (when WithOffset is true)
    /// </summary>
    public virtual string? OffsetAlias { get; }

    public BigQueryUnnestExpression(
        SqlExpression array,
        string alias,
        Type elementType,
        RelationalTypeMapping? elementTypeMapping,
        bool withOffset = false,
        string? offsetAlias = null)
        : base(alias)
    {
        Array = array;
        ElementType = elementType;
        ElementTypeMapping = elementTypeMapping;
        WithOffset = withOffset;
        OffsetAlias = offsetAlias;
    }

    protected override Expression VisitChildren(ExpressionVisitor visitor)
    {
        var array = (SqlExpression)visitor.Visit(Array);

        return Update(array);
    }

    public virtual BigQueryUnnestExpression Update(SqlExpression array)
    {
        return array != Array
            ? new BigQueryUnnestExpression(
                array,
                Alias!,
                ElementType,
                ElementTypeMapping,
                WithOffset,
                OffsetAlias)
            : this;
    }

    public override TableExpressionBase Clone(string? alias, ExpressionVisitor cloningExpressionVisitor)
    {
        var array = (SqlExpression)cloningExpressionVisitor.Visit(Array);

        return new BigQueryUnnestExpression(
            array,
            alias ?? Alias!,
            ElementType,
            ElementTypeMapping,
            WithOffset,
            OffsetAlias);
    }

    public override TableExpressionBase WithAlias(string newAlias)
    {
        return new BigQueryUnnestExpression(
            Array,
            newAlias,
            ElementType,
            ElementTypeMapping,
            WithOffset,
            OffsetAlias);
    }

    protected override TableExpressionBase WithAnnotations(IReadOnlyDictionary<string, IAnnotation> annotations)
    {
        return this;
    }

    protected override void Print(ExpressionPrinter expressionPrinter)
    {
        expressionPrinter.Append("UNNEST(");
        expressionPrinter.Visit(Array);
        expressionPrinter.Append(")");

        if (!string.IsNullOrEmpty(Alias))
        {
            expressionPrinter.Append(" AS ");
            expressionPrinter.Append(Alias);
        }

        if (WithOffset)
        {
            expressionPrinter.Append(" WITH OFFSET");
            if (!string.IsNullOrEmpty(OffsetAlias))
            {
                expressionPrinter.Append(" AS ");
                expressionPrinter.Append(OffsetAlias);
            }
        }
    }

    /// <inheritdoc />
    public override Expression Quote()
        => New(
            typeof(BigQueryUnnestExpression).GetConstructor(
                [
                    typeof(SqlExpression),
                    typeof(string),
                    typeof(Type),
                    typeof(RelationalTypeMapping),
                    typeof(bool),
                    typeof(string)
                ])!,
            Array.Quote(),
            Constant(Alias),
            Constant(ElementType),
            RelationalExpressionQuotingUtilities.QuoteTypeMapping(ElementTypeMapping),
            Constant(WithOffset),
            Constant(OffsetAlias, typeof(string)));

    public override bool Equals(object? obj)
        => obj != null
            && (ReferenceEquals(this, obj)
                || obj is BigQueryUnnestExpression unnestExpression
                    && Equals(unnestExpression));

    private bool Equals(BigQueryUnnestExpression unnestExpression)
        => base.Equals(unnestExpression)
            && Array.Equals(unnestExpression.Array)
            && ElementType == unnestExpression.ElementType
            && WithOffset == unnestExpression.WithOffset
            && OffsetAlias == unnestExpression.OffsetAlias;

    public override int GetHashCode()
        => HashCode.Combine(base.GetHashCode(), Array, ElementType, WithOffset, OffsetAlias);
}
