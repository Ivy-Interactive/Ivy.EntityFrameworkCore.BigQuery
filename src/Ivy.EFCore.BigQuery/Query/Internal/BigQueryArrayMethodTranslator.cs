using Ivy.EntityFrameworkCore.BigQuery.Storage.Internal.Mapping;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
using System.Reflection;

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal
{
    /// <summary>
    /// Translator for array methods to BigQuery array functions and operations
    /// </summary>
    public class BigQueryArrayMethodTranslator : IMethodCallTranslator
    {
        private readonly BigQuerySqlExpressionFactory _sqlExpressionFactory;
        private readonly IRelationalTypeMappingSource _typeMappingSource;

        #region MethodInfo Definitions

        private static readonly MethodInfo ArrayLengthGetter
            = typeof(Array).GetProperty(nameof(Array.Length))!.GetMethod!;

        private static readonly MethodInfo EnumerableCountWithoutPredicate
            = typeof(Enumerable).GetRuntimeMethods()
                .Single(m => m.Name == nameof(Enumerable.Count) &&
                             m.GetParameters().Length == 1 &&
                             m.IsGenericMethod);

        private static readonly MethodInfo EnumerableElementAt
            = typeof(Enumerable).GetRuntimeMethods()
                .Single(m => m.Name == nameof(Enumerable.ElementAt) &&
                             m.GetParameters().Length == 2 &&
                             m.IsGenericMethod &&
                             m.GetParameters()[1].ParameterType == typeof(int));

        private static readonly MethodInfo EnumerableFirstWithoutPredicate
            = typeof(Enumerable).GetRuntimeMethods()
                .Single(m => m.Name == nameof(Enumerable.First) &&
                             m.GetParameters().Length == 1 &&
                             m.IsGenericMethod);

        private static readonly MethodInfo EnumerableFirstOrDefaultWithoutPredicate
            = typeof(Enumerable).GetRuntimeMethods()
                .Single(m => m.Name == nameof(Enumerable.FirstOrDefault) &&
                             m.GetParameters().Length == 1 &&
                             m.IsGenericMethod);

        private static readonly MethodInfo EnumerableContains
            = typeof(Enumerable).GetRuntimeMethods()
                .Single(m => m.Name == nameof(Enumerable.Contains) &&
                             m.GetParameters().Length == 2 &&
                             m.IsGenericMethod);

        private static readonly MethodInfo EnumerableSequenceEqual
            = typeof(Enumerable).GetTypeInfo().GetMethods(BindingFlags.Public | BindingFlags.Static | BindingFlags.DeclaredOnly)
                .Single(m => m.Name == nameof(Enumerable.SequenceEqual) && m.GetParameters().Length == 2);

        private static readonly MethodInfo ListGetItem
            = typeof(List<>).GetProperty("Item")!.GetMethod!;

        private static readonly MethodInfo IListGetItem
            = typeof(IList<>).GetProperty("Item")!.GetMethod!;

        private static readonly MethodInfo EnumerableConcat
            = typeof(Enumerable).GetRuntimeMethods()
                .Single(m => m.Name == nameof(Enumerable.Concat) &&
                             m.GetParameters().Length == 2 &&
                             m.IsGenericMethod);

        private static readonly MethodInfo EnumerableReverse
            = typeof(Enumerable).GetRuntimeMethods()
                .Single(m => m.Name == nameof(Enumerable.Reverse) &&
                             m.GetParameters().Length == 1 &&
                             m.IsGenericMethod);

        #endregion

        public BigQueryArrayMethodTranslator(
            BigQuerySqlExpressionFactory sqlExpressionFactory,
            IRelationalTypeMappingSource typeMappingSource)
        {
            _sqlExpressionFactory = sqlExpressionFactory;
            _typeMappingSource = typeMappingSource;
        }

        public SqlExpression? Translate(
            SqlExpression? instance,
            MethodInfo method,
            IReadOnlyList<SqlExpression> arguments,
            IDiagnosticsLogger<DbLoggerCategory.Query> logger)
        {
            // byte[] - (BYTES)
            if (method.IsGenericMethod && arguments.Count >= 1 && IsByteArray(arguments[0]))
            {
                var genericMethod = method.GetGenericMethodDefinition();

                if (genericMethod == EnumerableElementAt)
                {
                    return TranslateByteArrayElementAt(arguments[0], arguments[1]);
                }

                if (genericMethod == EnumerableFirstWithoutPredicate
                    || genericMethod == EnumerableFirstOrDefaultWithoutPredicate)
                {
                    return TranslateByteArrayFirst(arguments[0]);
                }

                // byte[].Contains(value) -> value IN UNNEST(TO_CODE_POINTS(bytes))
                if (genericMethod == EnumerableContains)
                {
                    return TranslateByteArrayContains(arguments[0], arguments[1]);
                }
            }

            if (method.IsGenericMethod &&
                method.GetGenericMethodDefinition() == EnumerableSequenceEqual &&
                arguments.Count >= 2 &&
                IsArrayOrList(arguments[0]))
            {
                return TranslateSequenceEqual(arguments[0], arguments[1]);
            }

            if (method.IsGenericMethod &&
                method.GetGenericMethodDefinition() == EnumerableContains &&
                arguments.Count >= 2 &&
                IsArrayOrList(arguments[0]))
            {
                return TranslateContains(arguments[0], arguments[1]);
            }

            if (method.IsGenericMethod &&
                method.GetGenericMethodDefinition() == EnumerableConcat &&
                arguments.Count >= 2 &&
                IsArrayOrList(arguments[0]))
            {
                return TranslateConcat(arguments[0], arguments[1]);
            }

            if (method.IsGenericMethod &&
                method.GetGenericMethodDefinition() == EnumerableReverse &&
                arguments.Count >= 1 &&
                IsArrayOrList(arguments[0]))
            {
                return TranslateReverse(arguments[0]);
            }

            if (instance == null || !IsArrayOrList(instance))
            {
                return null;
            }

            if (method == ArrayLengthGetter || method.Name == "get_Length")
            {
                return TranslateLength(instance);
            }

            if (method.IsGenericMethod &&
                method.GetGenericMethodDefinition() == EnumerableCountWithoutPredicate)
            {
                return TranslateLength(instance);
            }

            if (method.IsGenericMethod &&
                method.GetGenericMethodDefinition() == EnumerableElementAt)
            {
                return TranslateElementAt(instance, arguments[1]);
            }

            // Array indexer arr[index] or List<T>[index] or IList<T>[index]
            if (method.Name == "get_Item" &&
                (method.DeclaringType?.IsArray == true ||
                 method.DeclaringType?.IsGenericType == true &&
                 (method.DeclaringType.GetGenericTypeDefinition() == typeof(List<>) ||
                  method.DeclaringType.GetGenericTypeDefinition() == typeof(IList<>))))
            {
                // byte[] indexer needs special handling
                if (IsByteArray(instance))
                {
                    return TranslateByteArrayElementAt(instance, arguments[0]);
                }

                return TranslateElementAt(instance, arguments[0]);
            }

            if (method.IsGenericMethod &&
                method.GetGenericMethodDefinition() == EnumerableFirstWithoutPredicate)
            {
                return TranslateFirst(instance);
            }

            if (method.IsGenericMethod &&
                method.GetGenericMethodDefinition() == EnumerableFirstOrDefaultWithoutPredicate)
            {
                return TranslateFirst(instance);
            }

            return null;
        }

        private bool IsArrayOrList(SqlExpression expression)
        {
            var type = expression.Type;
            return expression.TypeMapping is BigQueryArrayTypeMapping ||
                   (type.IsArray && type != typeof(byte[])) ||
                   (type.IsGenericType &&
                    type.GetGenericTypeDefinition() is Type genType &&
                    (genType == typeof(List<>) ||
                     genType == typeof(IList<>) ||
                     genType == typeof(ICollection<>) ||
                     genType == typeof(IReadOnlyList<>) ||
                     genType == typeof(IReadOnlyCollection<>)));
        }

        private bool IsByteArray(SqlExpression expression)
        {
            return expression.Type == typeof(byte[])
                && expression.TypeMapping is BigQueryByteArrayTypeMapping or null;
        }

        /// <summary>
        /// Translates byte[].ElementAt(n) to TO_CODE_POINTS(bytes)[OFFSET(n)]
        /// </summary>
        private SqlExpression TranslateByteArrayElementAt(SqlExpression byteArray, SqlExpression index)
        {
            // TO_CODE_POINTS(bytes)[OFFSET(index)]
            // We create the expression directly to avoid ArrayIndex validation
            // since TO_CODE_POINTS returns an array without BigQueryArrayTypeMapping
            var intTypeMapping = _typeMappingSource.FindMapping(typeof(int));
            var intArrayTypeMapping = _typeMappingSource.FindMapping(typeof(int[]));

            var codePoints = _sqlExpressionFactory.Function(
                "TO_CODE_POINTS",
                new[] { byteArray },
                nullable: true,
                argumentsPropagateNullability: new[] { true },
                typeof(int[]),
                intArrayTypeMapping);

            var typedIndex = _sqlExpressionFactory.ApplyTypeMapping(index, intTypeMapping);

            return new BigQueryArrayIndexExpression(codePoints, typedIndex, typeof(int), intTypeMapping);
        }

        /// <summary>
        /// Translates byte[].First() to TO_CODE_POINTS(bytes)[OFFSET(0)]
        /// </summary>
        private SqlExpression TranslateByteArrayFirst(SqlExpression byteArray)
        {
            return TranslateByteArrayElementAt(byteArray, _sqlExpressionFactory.Constant(0));
        }

        /// <summary>
        /// Translates byte[].Contains(value) to value IN UNNEST(TO_CODE_POINTS(bytes))
        /// BigQuery BYTES type needs to be converted to an array of code points first.
        /// </summary>
        private SqlExpression TranslateByteArrayContains(SqlExpression byteArray, SqlExpression value)
        {
            var intTypeMapping = _typeMappingSource.FindMapping(typeof(int));
            var intArrayTypeMapping = _typeMappingSource.FindMapping(typeof(int[]));

            // TO_CODE_POINTS(bytes) returns ARRAY<INT64>
            var codePoints = _sqlExpressionFactory.Function(
                "TO_CODE_POINTS",
                new[] { byteArray },
                nullable: true,
                argumentsPropagateNullability: new[] { true },
                typeof(int[]),
                intArrayTypeMapping);
            
            var typedValue = _sqlExpressionFactory.ApplyTypeMapping(
                _sqlExpressionFactory.Convert(value, typeof(int), intTypeMapping),
                intTypeMapping);

            // value IN UNNEST(TO_CODE_POINTS(bytes))
            return _sqlExpressionFactory.InUnnest(typedValue, codePoints);
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

        private SqlExpression TranslateElementAt(SqlExpression array, SqlExpression index)
        {
            return _sqlExpressionFactory.ArrayIndex(array, index);
        }

        private SqlExpression TranslateFirst(SqlExpression array)
        {
            // arr[OFFSET(0)]
            return _sqlExpressionFactory.ArrayIndex(
                array,
                _sqlExpressionFactory.Constant(0));
        }

        private SqlExpression TranslateSequenceEqual(SqlExpression array, SqlExpression other)
        {
            // arr1.SequenceEqual(arr2) => arr1 = arr2
            return _sqlExpressionFactory.Equal(array, other);
        }

        private SqlExpression TranslateContains(SqlExpression array, SqlExpression item)
        {
            if (array.TypeMapping == null)
            {
                var arrayTypeMapping = _typeMappingSource.FindMapping(array.Type);
                if (arrayTypeMapping != null)
                {
                    array = _sqlExpressionFactory.ApplyTypeMapping(array, arrayTypeMapping);
                }
            }

            //item IN UNNEST(array)
            return _sqlExpressionFactory.InUnnest(item, array);
        }

        private SqlExpression TranslateConcat(SqlExpression array1, SqlExpression array2)
        {
            // BigQuery: ARRAY_CONCAT(arr1, arr2)
            var arrayTypeMapping = array1.TypeMapping as BigQueryArrayTypeMapping
                ?? array2.TypeMapping as BigQueryArrayTypeMapping;

            return _sqlExpressionFactory.Function(
                "ARRAY_CONCAT",
                new[] { array1, array2 },
                nullable: true,
                argumentsPropagateNullability: new[] { true, true },
                array1.Type,
                arrayTypeMapping);
        }

        private SqlExpression TranslateReverse(SqlExpression array)
        {
            // ARRAY_REVERSE(arr)
            return _sqlExpressionFactory.Function(
                "ARRAY_REVERSE",
                new[] { array },
                nullable: true,
                argumentsPropagateNullability: new[] { true },
                array.Type,
                array.TypeMapping);
        }
    }
}
