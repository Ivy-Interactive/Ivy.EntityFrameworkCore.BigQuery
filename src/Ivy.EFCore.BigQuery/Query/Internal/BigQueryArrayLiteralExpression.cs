using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
using System.Linq.Expressions;

#pragma warning disable EF9100 // RelationalExpressionQuotingUtilities is for evaluation purposes
namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal
{
    /// <summary>
    /// Represents BigQuery array literal: ARRAY&lt;type&gt;[expr1, expr2, ...]
    /// </summary>
    public class BigQueryArrayLiteralExpression : SqlExpression
    {
        /// <summary>
        /// The array element expressions
        /// </summary>
        public virtual IReadOnlyList<SqlExpression> Elements { get; }

        /// <summary>
        /// The CLR element type
        /// </summary>
        public virtual Type ElementType { get; }

        /// <summary>
        /// The type mapping for array elements
        /// </summary>
        public virtual RelationalTypeMapping? ElementTypeMapping { get; }

        public BigQueryArrayLiteralExpression(
            IReadOnlyList<SqlExpression> elements,
            Type arrayType,
            Type elementType,
            RelationalTypeMapping? arrayTypeMapping,
            RelationalTypeMapping? elementTypeMapping)
            : base(arrayType, arrayTypeMapping)
        {
            Elements = elements;
            ElementType = elementType;
            ElementTypeMapping = elementTypeMapping;
        }

        /// <inheritdoc />
        protected override Expression VisitChildren(ExpressionVisitor visitor)
        {
            var newElements = new List<SqlExpression>();
            var changed = false;

            foreach (var element in Elements)
            {
                var visitedElement = (SqlExpression)visitor.Visit(element);
                newElements.Add(visitedElement);

                if (visitedElement != element)
                {
                    changed = true;
                }
            }

            return changed
                ? Update(newElements)
                : this;
        }

        public virtual BigQueryArrayLiteralExpression Update(IReadOnlyList<SqlExpression> elements)
        {
            return elements != Elements
                ? new BigQueryArrayLiteralExpression(elements, Type, ElementType, TypeMapping, ElementTypeMapping)
                : this;
        }

        /// <inheritdoc />
        protected override void Print(ExpressionPrinter expressionPrinter)
        {
            expressionPrinter.Append("ARRAY<");
            expressionPrinter.Append(ElementTypeMapping?.StoreType ?? ElementType.Name);
            expressionPrinter.Append(">[");

            for (int i = 0; i < Elements.Count; i++)
            {
                if (i > 0)
                {
                    expressionPrinter.Append(", ");
                }
                expressionPrinter.Visit(Elements[i]);
            }

            expressionPrinter.Append("]");
        }

        /// <inheritdoc />
        public override Expression Quote()
            => New(
                typeof(BigQueryArrayLiteralExpression).GetConstructor(
                    [
                        typeof(IReadOnlyList<SqlExpression>),
                        typeof(Type),
                        typeof(Type),
                        typeof(RelationalTypeMapping),
                        typeof(RelationalTypeMapping)
                    ])!,
                NewArrayInit(
                    typeof(SqlExpression),
                    Elements.Select(e => e.Quote())),
                Constant(Type),
                Constant(ElementType),
                RelationalExpressionQuotingUtilities.QuoteTypeMapping(TypeMapping),
                RelationalExpressionQuotingUtilities.QuoteTypeMapping(ElementTypeMapping));

        /// <inheritdoc />
        public override bool Equals(object? obj)
            => obj != null
                && (ReferenceEquals(this, obj)
                    || obj is BigQueryArrayLiteralExpression arrayLiteralExpression
                        && Equals(arrayLiteralExpression));

        private bool Equals(BigQueryArrayLiteralExpression arrayLiteralExpression)
            => base.Equals(arrayLiteralExpression)
                && ElementType == arrayLiteralExpression.ElementType
                && Elements.SequenceEqual(arrayLiteralExpression.Elements);

        /// <inheritdoc />
        public override int GetHashCode()
        {
            var hash = new HashCode();
            hash.Add(base.GetHashCode());
            hash.Add(ElementType);

            foreach (var element in Elements)
            {
                hash.Add(element);
            }

            return hash.ToHashCode();
        }
    }
}
