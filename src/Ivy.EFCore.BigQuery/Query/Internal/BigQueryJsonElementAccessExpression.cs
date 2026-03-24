using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
using System.Linq.Expressions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal;

/// <summary>
/// Represents property access on an unnested JSON element in BigQuery.
/// This is used when iterating over JSON arrays with UNNEST(JSON_QUERY_ARRAY(...)).
/// Each element is a JSON value, and properties are accessed via JSON_VALUE/JSON_QUERY.
/// </summary>
public class BigQueryJsonElementAccessExpression : SqlExpression
{
    /// <summary>
    /// The JSON element expression (typically a column from UNNEST)
    /// </summary>
    public virtual SqlExpression JsonElement { get; }

    /// <summary>
    /// The JSON property name to access
    /// </summary>
    public virtual string PropertyName { get; }

    /// <summary>
    /// Whether this is a scalar value (use JSON_VALUE) or an object/array (use JSON_QUERY)
    /// </summary>
    public virtual bool IsScalar { get; }

    public BigQueryJsonElementAccessExpression(
        SqlExpression jsonElement,
        string propertyName,
        Type type,
        RelationalTypeMapping? typeMapping,
        bool isScalar)
        : base(type, typeMapping)
    {
        JsonElement = jsonElement;
        PropertyName = propertyName;
        IsScalar = isScalar;
    }

    protected override Expression VisitChildren(ExpressionVisitor visitor)
    {
        var jsonElement = (SqlExpression)visitor.Visit(JsonElement);
        return Update(jsonElement);
    }

    public virtual BigQueryJsonElementAccessExpression Update(SqlExpression jsonElement)
    {
        return jsonElement != JsonElement
            ? new BigQueryJsonElementAccessExpression(jsonElement, PropertyName, Type, TypeMapping, IsScalar)
            : this;
    }

    /// <inheritdoc />
    public override Expression Quote()
        => throw new InvalidOperationException("BigQueryJsonElementAccessExpression.Quote() is not supported");

    protected override void Print(ExpressionPrinter expressionPrinter)
    {
        var functionName = IsScalar ? "JSON_VALUE" : "JSON_QUERY";
        expressionPrinter.Append($"{functionName}(");
        expressionPrinter.Visit(JsonElement);
        expressionPrinter.Append($", '$.{PropertyName}')");
    }

    public override bool Equals(object? obj)
        => obj is BigQueryJsonElementAccessExpression other && Equals(other);

    private bool Equals(BigQueryJsonElementAccessExpression other)
        => base.Equals(other)
           && JsonElement.Equals(other.JsonElement)
           && PropertyName == other.PropertyName
           && IsScalar == other.IsScalar;

    public override int GetHashCode()
        => HashCode.Combine(base.GetHashCode(), JsonElement, PropertyName, IsScalar);
}