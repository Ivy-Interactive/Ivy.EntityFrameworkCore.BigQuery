using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
using System.Linq.Expressions;
using static System.Linq.Expressions.Expression;

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal
{
    /// <summary>
    /// Represents BigQuery STRUCT field access: struct_column.field_name
    /// </summary>
    public class BigQueryStructAccessExpression : SqlExpression
    {
        /// <summary>
        /// The struct expression being accessed
        /// </summary>
        public virtual SqlExpression Struct { get; }

        /// <summary>
        /// The name of the field being accessed
        /// </summary>
        public virtual string FieldName { get; }

        public BigQueryStructAccessExpression(
            SqlExpression @struct,
            string fieldName,
            Type type,
            RelationalTypeMapping? typeMapping)
            : base(type, typeMapping)
        {
            Struct = @struct;
            FieldName = fieldName;
        }

        protected override Expression VisitChildren(ExpressionVisitor visitor)
        {
            var @struct = (SqlExpression)visitor.Visit(Struct);

            return Update(@struct);
        }

        public virtual BigQueryStructAccessExpression Update(SqlExpression @struct)
        {
            return @struct != Struct
                ? new BigQueryStructAccessExpression(@struct, FieldName, Type, TypeMapping)
                : this;
        }

        protected override void Print(ExpressionPrinter expressionPrinter)
        {
            expressionPrinter.Visit(Struct);
            expressionPrinter.Append(".");
            expressionPrinter.Append(FieldName);
        }

        /// <inheritdoc />
#pragma warning disable EF9100 // RelationalExpressionQuotingUtilities is for evaluation purposes
        public override Expression Quote()
            => New(
                typeof(BigQueryStructAccessExpression).GetConstructor(
                    [typeof(SqlExpression), typeof(string), typeof(Type), typeof(RelationalTypeMapping)])!,
                Struct.Quote(),
                Constant(FieldName),
                Constant(Type),
                RelationalExpressionQuotingUtilities.QuoteTypeMapping(TypeMapping));
#pragma warning restore EF9100

        public override bool Equals(object? obj)
            => obj != null
                && (ReferenceEquals(this, obj)
                    || obj is BigQueryStructAccessExpression structAccessExpression
                        && Equals(structAccessExpression));

        private bool Equals(BigQueryStructAccessExpression structAccessExpression)
            => base.Equals(structAccessExpression)
                && Struct.Equals(structAccessExpression.Struct)
                && FieldName == structAccessExpression.FieldName;

        public override int GetHashCode()
            => HashCode.Combine(base.GetHashCode(), Struct, FieldName);
    }
}
