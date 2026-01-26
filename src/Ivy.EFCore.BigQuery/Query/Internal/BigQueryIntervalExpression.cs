using System.Linq.Expressions;
using System.Reflection;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

#pragma warning disable EF9100 // RelationalExpressionQuotingUtilities is internal

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal;

/// <summary>
/// Represents a BigQuery INTERVAL expression (e.g., INTERVAL 1 HOUR).
/// This is needed because BigQuery's INTERVAL syntax uses spaces, not commas,
/// between the keyword, value, and date part.
/// </summary>
public class BigQueryIntervalExpression : SqlExpression
{
    private static ConstructorInfo? _quotingConstructor;

    /// <summary>
    /// The numeric value for the interval.
    /// </summary>
    public SqlExpression Value { get; }

    /// <summary>
    /// The date/time part (e.g., HOUR, DAY, MONTH).
    /// </summary>
    public string DatePart { get; }

    public BigQueryIntervalExpression(SqlExpression value, string datePart, RelationalTypeMapping? typeMapping)
        : base(typeof(TimeSpan), typeMapping)
    {
        Value = value;
        DatePart = datePart;
    }

    protected override Expression VisitChildren(ExpressionVisitor visitor)
    {
        var newValue = (SqlExpression)visitor.Visit(Value);
        return newValue != Value
            ? new BigQueryIntervalExpression(newValue, DatePart, TypeMapping)
            : this;
    }

    public override Expression Quote()
        => New(
            _quotingConstructor ??= typeof(BigQueryIntervalExpression).GetConstructor(
                [typeof(SqlExpression), typeof(string), typeof(RelationalTypeMapping)])!,
            Value.Quote(),
            Constant(DatePart),
            RelationalExpressionQuotingUtilities.QuoteTypeMapping(TypeMapping));

    protected override void Print(ExpressionPrinter expressionPrinter)
    {
        expressionPrinter.Append("INTERVAL ");
        expressionPrinter.Visit(Value);
        expressionPrinter.Append($" {DatePart}");
    }

    public override bool Equals(object? obj)
        => obj is BigQueryIntervalExpression other
           && base.Equals(other)
           && Value.Equals(other.Value)
           && DatePart == other.DatePart;

    public override int GetHashCode()
        => HashCode.Combine(base.GetHashCode(), Value, DatePart);
}