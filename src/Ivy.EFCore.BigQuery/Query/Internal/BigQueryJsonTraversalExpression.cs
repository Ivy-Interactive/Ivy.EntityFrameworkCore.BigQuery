using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
using System.Linq.Expressions;
using System.Reflection;

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal
{
    /// <summary>
    /// Represents a BigQuery JSON traversal expression using dot notation or subscript operators.
    /// Example: cart.name, cart.items[0], cart['fieldName']
    /// </summary>
    public class BigQueryJsonTraversalExpression : SqlExpression, IEquatable<BigQueryJsonTraversalExpression>
    {
        private static ConstructorInfo? _quotingConstructor;

        /// <summary>
        /// The base JSON expression (typically a column).
        /// </summary>
        public virtual SqlExpression Expression { get; }

        /// <summary>
        /// The path components to traverse (field names or array indices).
        /// </summary>
        public virtual IReadOnlyList<SqlExpression> Path { get; }

        /// <summary>
        /// Constructs a <see cref="BigQueryJsonTraversalExpression" />.
        /// </summary>
        public BigQueryJsonTraversalExpression(
            SqlExpression expression,
            IReadOnlyList<SqlExpression> path,
            Type type,
            RelationalTypeMapping? typeMapping)
            : base(type, typeMapping)
        {
            Expression = expression;
            Path = path;
        }

        /// <inheritdoc />
        protected override Expression VisitChildren(ExpressionVisitor visitor)
            => Update(
                (SqlExpression)visitor.Visit(Expression),
                Path.Select(p => (SqlExpression)visitor.Visit(p)).ToArray());

        /// <summary>
        /// Creates a new expression that is like this one, but using the supplied children.
        /// If all of the children are the same, it will return this expression.
        /// </summary>
        public virtual BigQueryJsonTraversalExpression Update(SqlExpression expression, IReadOnlyList<SqlExpression> path)
            => expression == Expression && path.Count == Path.Count && path.Zip(Path, (x, y) => (x, y)).All(tup => tup.x == tup.y)
                ? this
                : new BigQueryJsonTraversalExpression(expression, path, Type, TypeMapping);

        /// <inheritdoc />
        #pragma warning disable EF9100 // Justification: Using internal quoting utilities for expression serialization
        public override Expression Quote()
            => New(
                _quotingConstructor ??= typeof(BigQueryJsonTraversalExpression).GetConstructor(
                    new[] { typeof(SqlExpression), typeof(IReadOnlyList<SqlExpression>), typeof(Type), typeof(RelationalTypeMapping) })!,
                Expression.Quote(),
                NewArrayInit(typeof(SqlExpression), initializers: Path.Select(a => a.Quote())),
                Constant(Type),
                RelationalExpressionQuotingUtilities.QuoteTypeMapping(TypeMapping));
        #pragma warning restore EF9100

        /// <summary>
        /// Appends an additional path component to this <see cref="BigQueryJsonTraversalExpression" /> and returns the result.
        /// </summary>
        public virtual BigQueryJsonTraversalExpression Append(SqlExpression pathComponent)
        {
            var newPath = new SqlExpression[Path.Count + 1];
            for (var i = 0; i < Path.Count; i++)
            {
                newPath[i] = Path[i];
            }

            newPath[newPath.Length - 1] = pathComponent;
            return new BigQueryJsonTraversalExpression(Expression, newPath, Type, TypeMapping);
        }

        /// <inheritdoc />
        public override bool Equals(object? obj)
            => Equals(obj as BigQueryJsonTraversalExpression);

        /// <inheritdoc />
        public virtual bool Equals(BigQueryJsonTraversalExpression? other)
            => ReferenceEquals(this, other)
                || other is not null
                && base.Equals(other)
                && Equals(Expression, other.Expression)
                && Path.Count == other.Path.Count
                && Path.Zip(other.Path, (x, y) => (x, y)).All(tup => tup.x == tup.y);

        /// <inheritdoc />
        public override int GetHashCode()
            => HashCode.Combine(base.GetHashCode(), Expression, Path.Count);

        /// <inheritdoc />
        protected override void Print(ExpressionPrinter expressionPrinter)
        {
            expressionPrinter.Visit(Expression);
            foreach (var pathComponent in Path)
            {
                if (pathComponent is SqlConstantExpression { Value: string })
                {
                    expressionPrinter.Append(".");
                    expressionPrinter.Visit(pathComponent);
                }
                else
                {
                    expressionPrinter.Append("[");
                    expressionPrinter.Visit(pathComponent);
                    expressionPrinter.Append("]");
                }
            }
        }

        /// <inheritdoc />
        public override string ToString()
        {
            var pathStr = string.Join("", Path.Select(p =>
                p is SqlConstantExpression { Value: string s } ? $".{s}" : $"[{p}]"));
            return $"{Expression}{pathStr}";
        }
    }
}
