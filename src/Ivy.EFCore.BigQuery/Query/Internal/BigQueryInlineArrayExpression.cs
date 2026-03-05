using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
using System.Linq.Expressions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal;

/// <summary>
/// Represents an inline array literal in BigQuery: [elem1, elem2, ...]
/// </summary>
public class BigQueryInlineArrayExpression : SqlExpression
{
    public IReadOnlyList<SqlExpression> Elements { get; }

    public BigQueryInlineArrayExpression(
        IReadOnlyList<SqlExpression> elements,
        Type type,
        RelationalTypeMapping? typeMapping = null)
        : base(type, typeMapping)
    {
        Elements = elements;
    }

    protected override Expression VisitChildren(ExpressionVisitor visitor)
    {
        var changed = false;
        var newElements = new SqlExpression[Elements.Count];

        for (var i = 0; i < Elements.Count; i++)
        {
            newElements[i] = (SqlExpression)visitor.Visit(Elements[i]);
            changed |= newElements[i] != Elements[i];
        }

        return changed ? new BigQueryInlineArrayExpression(newElements, Type, TypeMapping) : this;
    }

    protected override void Print(ExpressionPrinter expressionPrinter)
    {
        expressionPrinter.Append("[");
        for (var i = 0; i < Elements.Count; i++)
        {
            if (i > 0)
            {
                expressionPrinter.Append(", ");
            }
            expressionPrinter.Visit(Elements[i]);
        }
        expressionPrinter.Append("]");
    }

    public override bool Equals(object? obj)
        => obj is BigQueryInlineArrayExpression other && Equals(other);

    private bool Equals(BigQueryInlineArrayExpression other)
        => base.Equals(other) && Elements.SequenceEqual(other.Elements);

    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(base.GetHashCode());
        foreach (var element in Elements)
        {
            hash.Add(element);
        }
        return hash.ToHashCode();
    }

    /// <inheritdoc />
    public override SqlExpression Quote()
        => new BigQueryInlineArrayExpression(Elements, Type, TypeMapping);
}