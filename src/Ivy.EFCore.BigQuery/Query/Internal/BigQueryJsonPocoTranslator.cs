using Ivy.EntityFrameworkCore.BigQuery.Storage.Internal.Mapping;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
using System.Reflection;
using System.Text.Json.Serialization;

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal
{
    /// <summary>
    /// Translates POCO property access on JSON columns to BigQuery SQL.
    /// Supports [JsonPropertyName] attribute.
    /// </summary>
    public class BigQueryJsonPocoTranslator : IMemberTranslator, IMethodCallTranslator
    {
        private readonly IRelationalTypeMappingSource _typeMappingSource;
        private readonly BigQuerySqlExpressionFactory _sqlExpressionFactory;
        private readonly RelationalTypeMapping _stringTypeMapping;
        private readonly IModel _model;

        private static readonly MethodInfo EnumerableAnyWithoutPredicate =
            typeof(Enumerable).GetTypeInfo().GetMethods(BindingFlags.Public | BindingFlags.Static | BindingFlags.DeclaredOnly)
                .Single(mi => mi.Name == nameof(Enumerable.Any) && mi.GetParameters().Length == 1);

        public BigQueryJsonPocoTranslator(
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
        /// Translates method calls like List&lt;T&gt;.Count, Any(), array indexers, etc.
        /// </summary>
        public virtual SqlExpression? Translate(
            SqlExpression? instance,
            MethodInfo method,
            IReadOnlyList<SqlExpression> arguments,
            IDiagnosticsLogger<DbLoggerCategory.Query> logger)
        {
            if (method.IsGenericMethod
                && method.GetGenericMethodDefinition() == EnumerableAnyWithoutPredicate)
            {
                var arrayLengthTranslation = TranslateArrayLength(arguments[0]);
                if (arrayLengthTranslation != null)
                {
                    return _sqlExpressionFactory.GreaterThan(arrayLengthTranslation, _sqlExpressionFactory.Constant(0));
                }
            }

            return null;
        }

        /// <summary>
        /// Translates member access on JSON-mapped POCOs.
        /// </summary>
        public virtual SqlExpression? Translate(
            SqlExpression? instance,
            MemberInfo member,
            Type returnType,
            IDiagnosticsLogger<DbLoggerCategory.Query> logger)
        {
            if (instance?.TypeMapping is not BigQueryJsonTypeMapping && instance is not BigQueryJsonTraversalExpression)
            {
                return null;
            }

            if (member is { Name: "Count", DeclaringType.IsGenericType: true }
                && member.DeclaringType.GetGenericTypeDefinition() == typeof(List<>))
            {
                return TranslateArrayLength(instance);
            }

            // Translate property access to JSON field access
            var fieldName = member.GetCustomAttribute<JsonPropertyNameAttribute>()?.Name ?? member.Name;
            return TranslateMemberAccess(
                instance,
                _sqlExpressionFactory.ApplyDefaultTypeMapping(_sqlExpressionFactory.Constant(fieldName)),
                returnType);
        }

        /// <summary>
        /// Translates member access to JSON field traversal.
        /// </summary>
        public virtual SqlExpression? TranslateMemberAccess(SqlExpression instance, SqlExpression member, Type returnType)
        {
            return instance switch
            {
                // First time seeing JSON traversal on a column - create JsonTraversalExpression
                ColumnExpression { TypeMapping: BigQueryJsonTypeMapping } columnExpression
                    => ConvertFromJson(
                        _sqlExpressionFactory.JsonTraversal(
                            columnExpression,
                            new[] { member },
                            typeof(object), // Will be converted later
                            columnExpression.TypeMapping),
                        returnType),

                // Append to existing traversal path
                BigQueryJsonTraversalExpression prevPathTraversal
                    => ConvertFromJson(
                        prevPathTraversal.Append(_sqlExpressionFactory.ApplyDefaultTypeMapping(member)),
                        returnType),

                _ => null
            };

            // BigQuery JSON traversal always returns JSON.
            // For scalar types, we need to wrap in type conversion function.
            SqlExpression ConvertFromJson(SqlExpression expression, Type returnType)
            {
                var unwrappedType = returnType.UnwrapNullableType();

                // Keep arrays and collections as JSON traversals (don't convert)
                // They need to stay as traversals so further operations (like indexing) can work
                if (unwrappedType.IsArray ||
                    (unwrappedType.IsGenericType &&
                     (unwrappedType.GetGenericTypeDefinition() == typeof(List<>) ||
                      unwrappedType.GetGenericTypeDefinition() == typeof(IList<>) ||
                      unwrappedType.GetGenericTypeDefinition() == typeof(IEnumerable<>) ||
                      unwrappedType.GetGenericTypeDefinition() == typeof(ICollection<>))))
                {
                    return expression;
                }

                var typeMapping = _typeMappingSource.FindMapping(unwrappedType, _model);

                if (typeMapping == null)
                {
                    // Not a scalar type - keep as JSON
                    return expression;
                }

                // Check if it's a JSON type itself
                if (typeMapping is BigQueryJsonTypeMapping)
                {
                    return expression;
                }

                // For scalar types, wrap in appropriate conversion function
                var functionName = GetConversionFunction(unwrappedType);
                if (functionName == null)
                {
                    // No conversion needed or not supported
                    return expression;
                }

                return new SqlFunctionExpression(
                    functionName,
                    new[] { expression },
                    nullable: true,
                    argumentsPropagateNullability: new[] { true },
                    returnType,
                    typeMapping);
            }
        }

        /// <summary>
        /// Gets the BigQuery function name for converting JSON to a specific type.
        /// </summary>
        private static string? GetConversionFunction(Type type)
        {
            return Type.GetTypeCode(type) switch
            {
                TypeCode.String => "STRING",
                TypeCode.Boolean => "BOOL",
                TypeCode.SByte or TypeCode.Byte or TypeCode.Int16 or TypeCode.UInt16 or
                TypeCode.Int32 or TypeCode.UInt32 or TypeCode.Int64 or TypeCode.UInt64 => "INT64",
                TypeCode.Single or TypeCode.Double or TypeCode.Decimal => "FLOAT64",
                _ => type == typeof(Guid) ? "STRING" : null
            };
        }

        /// <summary>
        /// Translates array length for JSON arrays using ARRAY_LENGTH(JSON_QUERY_ARRAY(...)).
        /// </summary>
        public virtual SqlExpression? TranslateArrayLength(SqlExpression expression)
        {
            switch (expression)
            {
                case ColumnExpression { TypeMapping: BigQueryJsonTypeMapping }:
                {
                    // For JSON column, need to extract as array first
                    var jsonQueryArray = new SqlFunctionExpression(
                        "JSON_QUERY_ARRAY",
                        new[] { expression },
                        nullable: true,
                        argumentsPropagateNullability: new[] { true },
                        typeof(object[]),
                        typeMapping: null);

                    return new SqlFunctionExpression(
                        "ARRAY_LENGTH",
                        new[] { jsonQueryArray },
                        nullable: true,
                        argumentsPropagateNullability: new[] { true },
                        typeof(int),
                        typeMapping: _typeMappingSource.FindMapping(typeof(int), _model));
                }

                case BigQueryJsonTraversalExpression traversal:
                {
                    // For traversal expression, apply JSON_QUERY_ARRAY then ARRAY_LENGTH
                    var jsonQueryArray = new SqlFunctionExpression(
                        "JSON_QUERY_ARRAY",
                        new[] { traversal },
                        nullable: true,
                        argumentsPropagateNullability: new[] { true },
                        typeof(object[]),
                        typeMapping: null);

                    return new SqlFunctionExpression(
                        "ARRAY_LENGTH",
                        new[] { jsonQueryArray },
                        nullable: true,
                        argumentsPropagateNullability: new[] { true },
                        typeof(int),
                        typeMapping: _typeMappingSource.FindMapping(typeof(int), _model));
                }

                default:
                    return null;
            }
        }
    }

    internal static class TypeExtensions
    {
        public static Type UnwrapNullableType(this Type type)
            => Nullable.GetUnderlyingType(type) ?? type;
    }
}