using Ivy.EntityFrameworkCore.BigQuery.Storage.Internal.Mapping;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
using System.Reflection;
using System.Text.Json;

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal
{
    /// <summary>
    /// Translates JsonElement and JsonDocument member access and method calls to BigQuery SQL.
    /// </summary>
    public class BigQueryJsonDomTranslator : IMemberTranslator, IMethodCallTranslator
    {
        private static readonly MemberInfo RootElement = typeof(JsonDocument).GetProperty(nameof(JsonDocument.RootElement))!;

        private static readonly MethodInfo GetProperty = typeof(JsonElement).GetRuntimeMethod(
            nameof(JsonElement.GetProperty), new[] { typeof(string) })!;

        private static readonly MethodInfo ArrayIndexer = typeof(JsonElement).GetProperties()
            .Single(p => p.GetIndexParameters().Length == 1 && p.GetIndexParameters()[0].ParameterType == typeof(int))
            .GetMethod!;

        private static readonly string[] GetMethods =
        {
            nameof(JsonElement.GetBoolean),
            nameof(JsonElement.GetDateTime),
            nameof(JsonElement.GetDateTimeOffset),
            nameof(JsonElement.GetDecimal),
            nameof(JsonElement.GetDouble),
            nameof(JsonElement.GetGuid),
            nameof(JsonElement.GetInt16),
            nameof(JsonElement.GetInt32),
            nameof(JsonElement.GetInt64),
            nameof(JsonElement.GetSingle),
            nameof(JsonElement.GetString)
        };

        private readonly IRelationalTypeMappingSource _typeMappingSource;
        private readonly BigQuerySqlExpressionFactory _sqlExpressionFactory;
        private readonly RelationalTypeMapping _stringTypeMapping;
        private readonly IModel _model;

        public BigQueryJsonDomTranslator(
            IRelationalTypeMappingSource typeMappingSource,
            BigQuerySqlExpressionFactory sqlExpressionFactory,
            IModel model)
        {
            _typeMappingSource = typeMappingSource;
            _sqlExpressionFactory = sqlExpressionFactory;
            _model = model;
            _stringTypeMapping = typeMappingSource.FindMapping(typeof(string), model)!;
        }

        /// <summary>
        /// Translates member access on JsonDocument (e.g., RootElement).
        /// </summary>
        public virtual SqlExpression? Translate(
            SqlExpression? instance,
            MemberInfo member,
            Type returnType,
            IDiagnosticsLogger<DbLoggerCategory.Query> logger)
        {
            if (member.DeclaringType != typeof(JsonDocument))
            {
                return null;
            }

            if (member == RootElement && instance is ColumnExpression column && IsJsonTypeMapping(column.TypeMapping))
            {
                return _sqlExpressionFactory.JsonTraversal(
                    column,
                    Array.Empty<SqlExpression>(),
                    typeof(JsonElement),
                    column.TypeMapping);
            }

            return null;
        }

        /// <summary>
        /// Translates method calls on JsonElement (e.g., GetProperty, indexer, GetString, GetInt64, etc.).
        /// </summary>
        public virtual SqlExpression? Translate(
            SqlExpression? instance,
            MethodInfo method,
            IReadOnlyList<SqlExpression> arguments,
            IDiagnosticsLogger<DbLoggerCategory.Query> logger)
        {
            if (method.DeclaringType != typeof(JsonElement) || !IsJsonTypeMapping(instance?.TypeMapping))
            {
                return null;
            }

            var mapping = instance!.TypeMapping;

            if (instance is ColumnExpression columnExpression)
            {
                instance = _sqlExpressionFactory.JsonTraversal(
                    columnExpression, typeof(JsonElement), mapping);
            }

            if (method == GetProperty)
            {
                return instance is BigQueryJsonTraversalExpression prevPathTraversal
                    ? prevPathTraversal.Append(_sqlExpressionFactory.ApplyDefaultTypeMapping(arguments[0]))
                    : null;
            }

            if (method == ArrayIndexer)
            {
                return instance is BigQueryJsonTraversalExpression prevPathTraversal
                    ? prevPathTraversal.Append(_sqlExpressionFactory.ApplyDefaultTypeMapping(arguments[0]))
                    : null;
            }

            // GetXxx() methods - wrap traversal in type conversion function
            if (GetMethods.Contains(method.Name) && arguments.Count == 0 && instance is BigQueryJsonTraversalExpression traversal)
            {
                return method.Name switch
                {
                    nameof(JsonElement.GetString) => WrapInFunction("STRING", traversal, typeof(string)),
                    nameof(JsonElement.GetBoolean) => WrapInFunction("BOOL", traversal, typeof(bool)),
                    nameof(JsonElement.GetInt16) => WrapInFunction("INT64", traversal, typeof(short)),
                    nameof(JsonElement.GetInt32) => WrapInFunction("INT64", traversal, typeof(int)),
                    nameof(JsonElement.GetInt64) => WrapInFunction("INT64", traversal, typeof(long)),
                    nameof(JsonElement.GetSingle) => WrapInFunction("FLOAT64", traversal, typeof(float)),
                    nameof(JsonElement.GetDouble) => WrapInFunction("FLOAT64", traversal, typeof(double)),
                    nameof(JsonElement.GetDecimal) => WrapInFunction("FLOAT64", traversal, typeof(decimal)),
                    nameof(JsonElement.GetDateTime) => WrapInFunction("STRING", traversal, typeof(string)),
                    nameof(JsonElement.GetDateTimeOffset) => WrapInFunction("STRING", traversal, typeof(string)),
                    nameof(JsonElement.GetGuid) => WrapInFunction("STRING", traversal, typeof(string)),
                    _ => null
                };
            }

            //Todo: implement
            if (method.Name.StartsWith("TryGet", StringComparison.Ordinal) && arguments.Count == 0)
            {
                throw new InvalidOperationException(
                    $"The TryGet* methods on {nameof(JsonElement)} aren't translated yet, use Get* instead.");
            }

            return null;
        }

        private SqlExpression WrapInFunction(string functionName, BigQueryJsonTraversalExpression traversal, Type returnType)
        {
            var typeMapping = _typeMappingSource.FindMapping(returnType, _model);

            return new SqlFunctionExpression(
                functionName,
                new[] { traversal },
                nullable: true,
                argumentsPropagateNullability: new[] { true },
                returnType,
                typeMapping);
        }

        private static bool IsJsonTypeMapping(RelationalTypeMapping? typeMapping)
            => typeMapping is BigQueryJsonTypeMapping or BigQueryOwnedJsonTypeMapping;
    }
}