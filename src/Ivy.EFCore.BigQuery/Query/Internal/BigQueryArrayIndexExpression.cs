using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
using System.Linq.Expressions;

#pragma warning disable EF9100 // RelationalExpressionQuotingUtilities is for evaluation purposes
namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal
{
    /// <summary>
    /// ARRAY element access: array[OFFSET(index)]
    /// </summary>
    public class BigQueryArrayIndexExpression : SqlExpression
    {
        /// <summary>
        /// The array expression being indexed
        /// </summary>
        public virtual SqlExpression Array { get; }

        /// <summary>
        /// The index expression (zero-based)
        /// </summary>
        public virtual SqlExpression Index { get; }

        public BigQueryArrayIndexExpression(
            SqlExpression array,
            SqlExpression index,
            Type type,
            RelationalTypeMapping? typeMapping)
            : base(type, typeMapping)
        {
            Array = array;
            Index = index;
        }

        protected override Expression VisitChildren(ExpressionVisitor visitor)
        {
            var array = (SqlExpression)visitor.Visit(Array);
            var index = (SqlExpression)visitor.Visit(Index);

            return Update(array, index);
        }

        public virtual BigQueryArrayIndexExpression Update(SqlExpression array, SqlExpression index)
        {
            return array != Array || index != Index
                ? new BigQueryArrayIndexExpression(array, index, Type, TypeMapping)
                : this;
        }

        protected override void Print(ExpressionPrinter expressionPrinter)
        {
            expressionPrinter.Visit(Array);
            expressionPrinter.Append("[OFFSET(");
            expressionPrinter.Visit(Index);
            expressionPrinter.Append(")]");
        }

        /// <inheritdoc />
        public override Expression Quote()
            => New(
                typeof(BigQueryArrayIndexExpression).GetConstructor(
                    [typeof(SqlExpression), typeof(SqlExpression), typeof(Type), typeof(RelationalTypeMapping)])!,
                Array.Quote(),
                Index.Quote(),
                Constant(Type),
                RelationalExpressionQuotingUtilities.QuoteTypeMapping(TypeMapping));


        /// <inheritdoc />
        public override bool Equals(object? obj)
            => obj != null
                && (ReferenceEquals(this, obj)
                    || obj is BigQueryArrayIndexExpression arrayIndexExpression
                        && Equals(arrayIndexExpression));

        private bool Equals(BigQueryArrayIndexExpression arrayIndexExpression)
            => base.Equals(arrayIndexExpression)
                && Array.Equals(arrayIndexExpression.Array)
                && Index.Equals(arrayIndexExpression.Index);

        public override int GetHashCode()
            => HashCode.Combine(base.GetHashCode(), Array, Index);
    }
}
