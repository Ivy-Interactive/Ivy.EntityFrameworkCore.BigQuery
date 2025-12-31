using Ivy.EntityFrameworkCore.BigQuery.Storage.Internal.Mapping;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using System.Reflection;

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal
{
    /// <summary>
    /// Translator for array member access (properties) to BigQuery array functions
    /// </summary>
    public class BigQueryArrayMemberTranslator : IMemberTranslator
    {
        private readonly ISqlExpressionFactory _sqlExpressionFactory;

        public BigQueryArrayMemberTranslator(ISqlExpressionFactory sqlExpressionFactory)
        {
            _sqlExpressionFactory = sqlExpressionFactory;
        }

        public SqlExpression? Translate(
            SqlExpression? instance,
            MemberInfo member,
            Type returnType,
            IDiagnosticsLogger<DbLoggerCategory.Query> logger)
        {
            // Only translate if instance is an array or list type
            if (instance == null)
            {
                return null;
            }

            // Type mapping might not be set yet when translator is called
            var isArrayType = instance.TypeMapping is BigQueryArrayTypeMapping ||
                             instance.Type.IsArray ||
                             (instance.Type.IsGenericType &&
                              (instance.Type.GetGenericTypeDefinition() == typeof(List<>) ||
                               instance.Type.GetGenericTypeDefinition() == typeof(IList<>) ||
                               instance.Type.GetGenericTypeDefinition() == typeof(ICollection<>) ||
                               instance.Type.GetGenericTypeDefinition() == typeof(IReadOnlyList<>) ||
                               instance.Type.GetGenericTypeDefinition() == typeof(IReadOnlyCollection<>)));

            if (!isArrayType)
            {
                return null;
            }

            if (member.Name == nameof(Array.Length) && member.DeclaringType?.IsArray == true)
            {
                return TranslateLength(instance);
            }

            // List<T>.Count
            if (member.Name == "Count" && member.DeclaringType?.IsGenericType == true)
            {
                var genericDef = member.DeclaringType.GetGenericTypeDefinition();
                if (genericDef == typeof(List<>) ||
                    genericDef == typeof(IList<>) ||
                    genericDef == typeof(ICollection<>) ||
                    genericDef == typeof(IReadOnlyList<>) ||
                    genericDef == typeof(IReadOnlyCollection<>))
                {
                    return TranslateLength(instance);
                }
            }

            return null;
        }

        private SqlExpression TranslateLength(SqlExpression array)
        {
            return _sqlExpressionFactory.Function(
                "ARRAY_LENGTH",
                new[] { array },
                nullable: true,
                argumentsPropagateNullability: new[] { true },
                typeof(int));
        }
    }
}
