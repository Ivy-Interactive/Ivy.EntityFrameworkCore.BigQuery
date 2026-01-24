using System.Linq.Expressions;
using System.Reflection;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

#pragma warning disable EF9100 // RelationalExpressionQuotingUtilities is internal

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal;

/// <summary>
/// Represents a BigQuery EXTRACT expression: EXTRACT(part FROM datetime_expression)
/// EXTRACT has special syntax without commas between arguments.
/// </summary>
public class BigQueryExtractExpression : SqlExpression
{
    private static ConstructorInfo? _quotingConstructor;

    public string Part { get; }
    public SqlExpression Argument { get; }

    public BigQueryExtractExpression(string part, SqlExpression argument, Type type, RelationalTypeMapping? typeMapping = null)
        : base(type, typeMapping)
    {
        Part = part;
        Argument = argument;
    }

    protected override Expression VisitChildren(ExpressionVisitor visitor)
    {
        var argument = (SqlExpression)visitor.Visit(Argument);

        return argument != Argument
            ? new BigQueryExtractExpression(Part, argument, Type, TypeMapping)
            : this;
    }

    public BigQueryExtractExpression ApplyTypeMapping(RelationalTypeMapping? typeMapping)
        => new(Part, Argument, Type, typeMapping);

    /// <inheritdoc />

    public override Expression Quote()
        => New(
            _quotingConstructor ??= typeof(BigQueryExtractExpression).GetConstructor(
                [typeof(string), typeof(SqlExpression), typeof(Type), typeof(RelationalTypeMapping)])!,
            Constant(Part),
            Argument.Quote(),
            Constant(Type),
            RelationalExpressionQuotingUtilities.QuoteTypeMapping(TypeMapping));
#pragma warning restore EF9100

    protected override void Print(ExpressionPrinter expressionPrinter)
    {
        expressionPrinter.Append($"EXTRACT({Part} FROM ");
        expressionPrinter.Visit(Argument);
        expressionPrinter.Append(")");
    }

    public override bool Equals(object? obj)
        => obj is BigQueryExtractExpression other
            && Part == other.Part
            && Argument.Equals(other.Argument);

    public override int GetHashCode()
        => HashCode.Combine(Part, Argument);
}