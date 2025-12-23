using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
using System.Linq.Expressions;
using static System.Linq.Expressions.Expression;

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal
{
    /// <summary>
    /// Represents BigQuery STRUCT constructor:
    /// - STRUCT&lt;field1 TYPE1, field2 TYPE2&gt;(value1, value2)
    /// - STRUCT(value1 AS field1, value2 AS field2)
    /// </summary>
    public class BigQueryStructConstructorExpression : SqlExpression
    {
        /// <summary>
        /// The field values for the struct
        /// </summary>
        public virtual IReadOnlyList<SqlExpression> Arguments { get; }

        /// <summary>
        /// The field names for the struct
        /// </summary>
        public virtual IReadOnlyList<string> FieldNames { get; }

        /// <summary>
        /// The type mappings for each field
        /// </summary>
        public virtual IReadOnlyList<RelationalTypeMapping?> FieldTypeMappings { get; }

        /// <summary>
        /// Optional explicit type specification like "STRUCT&lt;name STRING, age INT64&gt;"
        /// If null, will use named field syntax: STRUCT(value AS name, ...)
        /// </summary>
        public virtual string? ExplicitType { get; }

        public BigQueryStructConstructorExpression(
            IReadOnlyList<SqlExpression> arguments,
            IReadOnlyList<string> fieldNames,
            IReadOnlyList<RelationalTypeMapping?> fieldTypeMappings,
            Type type,
            RelationalTypeMapping? typeMapping,
            string? explicitType = null)
            : base(type, typeMapping)
        {
            if (arguments.Count != fieldNames.Count)
            {
                throw new ArgumentException(
                    $"Arguments count ({arguments.Count}) must match field names count ({fieldNames.Count})");
            }

            if (arguments.Count != fieldTypeMappings.Count)
            {
                throw new ArgumentException(
                    $"Arguments count ({arguments.Count}) must match field type mappings count ({fieldTypeMappings.Count})");
            }

            Arguments = arguments;
            FieldNames = fieldNames;
            FieldTypeMappings = fieldTypeMappings;
            ExplicitType = explicitType;
        }

        protected override Expression VisitChildren(ExpressionVisitor visitor)
        {
            var changed = false;
            var arguments = new SqlExpression[Arguments.Count];

            for (var i = 0; i < Arguments.Count; i++)
            {
                arguments[i] = (SqlExpression)visitor.Visit(Arguments[i]);
                changed |= arguments[i] != Arguments[i];
            }

            return changed ? Update(arguments) : this;
        }

        public virtual BigQueryStructConstructorExpression Update(IReadOnlyList<SqlExpression> arguments)
        {
            return !arguments.SequenceEqual(Arguments)
                ? new BigQueryStructConstructorExpression(
                    arguments, FieldNames, FieldTypeMappings, Type, TypeMapping, ExplicitType)
                : this;
        }

        protected override void Print(ExpressionPrinter expressionPrinter)
        {
            expressionPrinter.Append("STRUCT");

            if (!string.IsNullOrEmpty(ExplicitType))
            {
                expressionPrinter.Append("<");
                expressionPrinter.Append(ExplicitType);
                expressionPrinter.Append(">");
            }

            expressionPrinter.Append("(");

            for (var i = 0; i < Arguments.Count; i++)
            {
                if (i > 0)
                {
                    expressionPrinter.Append(", ");
                }

                expressionPrinter.Visit(Arguments[i]);

                if (string.IsNullOrEmpty(ExplicitType) && !string.IsNullOrEmpty(FieldNames[i]))
                {
                    expressionPrinter.Append(" AS ");
                    expressionPrinter.Append(FieldNames[i]);
                }
            }

            expressionPrinter.Append(")");
        }

        /// <inheritdoc />
#pragma warning disable EF9100 // RelationalExpressionQuotingUtilities is for evaluation purposes
        public override Expression Quote()
            => New(
                typeof(BigQueryStructConstructorExpression).GetConstructor(
                    [typeof(IReadOnlyList<SqlExpression>), typeof(IReadOnlyList<string>),
                     typeof(IReadOnlyList<RelationalTypeMapping>), typeof(Type),
                     typeof(RelationalTypeMapping), typeof(string)])!,
                NewArrayInit(typeof(SqlExpression), Arguments.Select(a => a.Quote())),
                Constant(FieldNames),
                Constant(FieldTypeMappings),
                Constant(Type),
                RelationalExpressionQuotingUtilities.QuoteTypeMapping(TypeMapping),
                Constant(ExplicitType));
#pragma warning restore EF9100

        public override bool Equals(object? obj)
            => obj != null
                && (ReferenceEquals(this, obj)
                    || obj is BigQueryStructConstructorExpression structConstructorExpression
                        && Equals(structConstructorExpression));

        private bool Equals(BigQueryStructConstructorExpression structConstructorExpression)
            => base.Equals(structConstructorExpression)
                && Arguments.SequenceEqual(structConstructorExpression.Arguments)
                && FieldNames.SequenceEqual(structConstructorExpression.FieldNames)
                && ExplicitType == structConstructorExpression.ExplicitType;

        public override int GetHashCode()
        {
            var hash = new HashCode();
            hash.Add(base.GetHashCode());

            foreach (var argument in Arguments)
            {
                hash.Add(argument);
            }

            foreach (var fieldName in FieldNames)
            {
                hash.Add(fieldName);
            }

            hash.Add(ExplicitType);

            return hash.ToHashCode();
        }
    }
}
