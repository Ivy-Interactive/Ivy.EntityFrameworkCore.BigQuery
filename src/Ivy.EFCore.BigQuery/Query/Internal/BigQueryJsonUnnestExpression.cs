using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using System.Linq.Expressions;

#pragma warning disable EF9100 // RelationalExpressionQuotingUtilities is for evaluation purposes

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal;

/// <summary>
/// Represents BigQuery UNNEST expression for JSON arrays: UNNEST(JSON_QUERY_ARRAY(json_col, path)) AS alias
/// This expression represents the expansion of a JSON array into rows for querying.
/// </summary>
public class BigQueryJsonUnnestExpression : TableExpressionBase
{
    /// <summary>
    /// The JSON array expression being unnested (typically a JSON_QUERY_ARRAY call)
    /// </summary>
    public virtual SqlExpression JsonArrayExpression { get; }

    /// <summary>
    /// The original JsonQueryExpression this was derived from
    /// </summary>
    public virtual JsonQueryExpression JsonQueryExpression { get; }

    /// <summary>
    /// Whether to include WITH OFFSET clause
    /// </summary>
    public virtual bool WithOffset { get; }

    /// <summary>
    /// Alias for the offset column (when WithOffset is true)
    /// </summary>
    public virtual string? OffsetAlias { get; }

    public BigQueryJsonUnnestExpression(
        SqlExpression jsonArrayExpression,
        string alias,
        JsonQueryExpression jsonQueryExpression,
        bool withOffset = false,
        string? offsetAlias = null)
        : base(alias)
    {
        JsonArrayExpression = jsonArrayExpression;
        JsonQueryExpression = jsonQueryExpression;
        WithOffset = withOffset;
        OffsetAlias = offsetAlias;
    }

    protected override Expression VisitChildren(ExpressionVisitor visitor)
    {
        return this;
    }

    public virtual BigQueryJsonUnnestExpression Update(SqlExpression jsonArrayExpression)
    {
        return jsonArrayExpression != JsonArrayExpression
            ? new BigQueryJsonUnnestExpression(
                jsonArrayExpression,
                Alias!,
                JsonQueryExpression,
                WithOffset,
                OffsetAlias)
            : this;
    }

    public override TableExpressionBase Clone(string? alias, ExpressionVisitor cloningExpressionVisitor)
    {
        var jsonArrayExpression = (SqlExpression)cloningExpressionVisitor.Visit(JsonArrayExpression);

        return new BigQueryJsonUnnestExpression(
            jsonArrayExpression,
            alias ?? Alias!,
            JsonQueryExpression,
            WithOffset,
            OffsetAlias);
    }

    public override TableExpressionBase WithAlias(string newAlias)
    {
        return new BigQueryJsonUnnestExpression(
            JsonArrayExpression,
            newAlias,
            JsonQueryExpression,
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
        expressionPrinter.Visit(JsonArrayExpression);
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

    public override Expression Quote()
        => throw new InvalidOperationException("BigQueryJsonUnnestExpression.Quote() is not supported");

    public override bool Equals(object? obj)
        => obj != null
            && (ReferenceEquals(this, obj)
                || obj is BigQueryJsonUnnestExpression unnestExpression
                    && Equals(unnestExpression));

    private bool Equals(BigQueryJsonUnnestExpression unnestExpression)
        => base.Equals(unnestExpression)
            && JsonArrayExpression.Equals(unnestExpression.JsonArrayExpression)
            && WithOffset == unnestExpression.WithOffset
            && OffsetAlias == unnestExpression.OffsetAlias;

    public override int GetHashCode()
        => HashCode.Combine(base.GetHashCode(), JsonArrayExpression, WithOffset, OffsetAlias);
}