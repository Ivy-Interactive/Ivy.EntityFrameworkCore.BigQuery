using Ivy.EntityFrameworkCore.BigQuery.Extensions;
using Ivy.EntityFrameworkCore.BigQuery.Storage.Internal.Mapping;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
using System.Reflection;

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal
{
    /// <summary>
    /// Translates BigQuery JSON function calls (e.g., EF.Functions.JsonType) to SQL.
    /// </summary>
    public class BigQueryJsonDbFunctionsTranslator : IMethodCallTranslator
    {
        private readonly BigQuerySqlExpressionFactory _sqlExpressionFactory;
        private readonly RelationalTypeMapping _stringTypeMapping;

        public BigQueryJsonDbFunctionsTranslator(
            IRelationalTypeMappingSource typeMappingSource,
            BigQuerySqlExpressionFactory sqlExpressionFactory,
            IModel model)
        {
            _sqlExpressionFactory = sqlExpressionFactory;
            _stringTypeMapping = typeMappingSource.FindMapping(typeof(string), model)!;
        }

        public virtual SqlExpression? Translate(
            SqlExpression? instance,
            MethodInfo method,
            IReadOnlyList<SqlExpression> arguments,
            IDiagnosticsLogger<DbLoggerCategory.Query> logger)
        {
            if (method.DeclaringType != typeof(BigQueryJsonDbFunctionsExtensions))
            {
                return null;
            }

            var args = arguments
                // DbFunctions instance parameter
                .Skip(1)
                // Remove any Convert nodes wrapping the arguments
                .Select(RemoveConvert)
                // Ensure we have BigQueryJsonTraversalExpression instead of nested column expressions
                .Select(UnwrapJsonColumn)
                .ToArray();

            // Verify we have at least one JSON argument
            if (!args.Any(a => a.TypeMapping is BigQueryJsonTypeMapping || a is BigQueryJsonTraversalExpression))
            {
                throw new InvalidOperationException("BigQuery JSON methods require a JSON parameter and none was found.");
            }

            return method.Name switch
            {
                nameof(BigQueryJsonDbFunctionsExtensions.JsonType)
                    => new SqlFunctionExpression(
                        "JSON_TYPE",
                        new[] { args[0] },
                        nullable: true,
                        argumentsPropagateNullability: new[] { true },
                        typeof(string),
                        _stringTypeMapping),

                nameof(BigQueryJsonDbFunctionsExtensions.JsonKeys)
                    => new SqlFunctionExpression(
                        "JSON_KEYS",
                        new[] { args[0] },
                        nullable: true,
                        argumentsPropagateNullability: new[] { true },
                        typeof(string[]),
                        null),

                _ => null
            };

            static SqlExpression RemoveConvert(SqlExpression expression)
            {
                while (expression is SqlUnaryExpression { OperatorType: System.Linq.Expressions.ExpressionType.Convert or System.Linq.Expressions.ExpressionType.ConvertChecked } unary)
                {
                    expression = unary.Operand;
                }
                return expression;
            }

            SqlExpression UnwrapJsonColumn(SqlExpression expression)
            {
                if (expression is ColumnExpression { TypeMapping: BigQueryJsonTypeMapping mapping } column)
                {
                    return _sqlExpressionFactory.JsonTraversal(column, typeof(object), mapping);
                }

                return expression;
            }
        }
    }
}
