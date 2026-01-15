using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.Json;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using Microsoft.EntityFrameworkCore.Utilities;
using System.Collections;
using System.Data;
using System.Data.Common;
using System.Text;

namespace Ivy.EntityFrameworkCore.BigQuery.Storage.Internal.Mapping
{
    /// <summary>
    /// Type mapping for BigQuery arrays.
    /// </summary>
    public abstract class BigQueryArrayTypeMapping : RelationalTypeMapping
    {
        protected BigQueryArrayTypeMapping(RelationalTypeMappingParameters parameters)
            : base(parameters)
        {
        }

        public override RelationalTypeMapping ElementTypeMapping
        {
            get
            {
                var elementTypeMapping = base.ElementTypeMapping;
                if (elementTypeMapping is null)
                    throw new InvalidOperationException("BigQueryArrayTypeMapping without an element type mapping");
                return (RelationalTypeMapping)elementTypeMapping;
            }
        }
    }

    /// <summary>
    /// Type mapping for BigQuery arrays.
    /// </summary>
    public class BigQueryArrayTypeMapping<TCollection, TConcreteCollection, TElement> : BigQueryArrayTypeMapping
        where TConcreteCollection : class, IEnumerable<TElement>
    {
        public static BigQueryArrayTypeMapping<TCollection, TConcreteCollection, TElement> Default { get; }
            = new();

        public BigQueryArrayTypeMapping(RelationalTypeMapping elementTypeMapping)
            : this($"ARRAY<{elementTypeMapping.StoreType}>", elementTypeMapping)
        {
        }

        public BigQueryArrayTypeMapping(string storeType, RelationalTypeMapping elementTypeMapping)
            : this(CreateParameters(storeType, elementTypeMapping))
        {
        }

        private static RelationalTypeMappingParameters CreateParameters(string storeType, RelationalTypeMapping elementMapping)
        {
            var elementType = typeof(TElement);

            Microsoft.EntityFrameworkCore.Storage.ValueConversion.ValueConverter? converter = null;
            if (typeof(TCollection) != typeof(TConcreteCollection))
            {
                // Create converter for interface types (e.g., IList<T> to T[])
                converter = new Microsoft.EntityFrameworkCore.Storage.ValueConversion.ValueConverter<TCollection, TConcreteCollection>(
                    v => ConvertToConcreteCollection(v),
                    v => ConvertToCollection(v));
            }

            var comparer = typeof(TCollection).IsArray && typeof(TCollection).GetArrayRank() > 1
                ? null // Multidimensional arrays not supported for comparers
                : CreateValueComparer(elementMapping.Comparer);

            JsonValueReaderWriter? jsonValueReaderWriter = null;
            if (elementMapping.JsonValueReaderWriter != null)
            {
                Type collectionReaderWriterType;
                Type elementTypeArgument;

                if (typeof(TElement).IsValueType)
                {
                    var underlyingType = Nullable.GetUnderlyingType(typeof(TElement));
                    if (underlyingType != null)
                    {
                        collectionReaderWriterType = typeof(JsonCollectionOfNullableStructsReaderWriter<,>);
                        elementTypeArgument = underlyingType;
                    }
                    else
                    {
                        collectionReaderWriterType = typeof(JsonCollectionOfStructsReaderWriter<,>);
                        elementTypeArgument = typeof(TElement);
                    }
                }
                else
                {
                    collectionReaderWriterType = typeof(JsonCollectionOfReferencesReaderWriter<,>);
                    elementTypeArgument = typeof(TElement);
                }

                var genericType = collectionReaderWriterType.MakeGenericType(typeof(TConcreteCollection), elementTypeArgument);
                jsonValueReaderWriter = (JsonValueReaderWriter)Activator.CreateInstance(
                    genericType,
                    elementMapping.JsonValueReaderWriter)!;
            }

            return new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(
                    typeof(TCollection),
                    converter,
                    comparer,
                    jsonValueReaderWriter: jsonValueReaderWriter,
                    elementMapping: elementMapping),
                storeType);
        }

        private static TConcreteCollection ConvertToConcreteCollection(TCollection value)
        {
            return value switch
            {
                TConcreteCollection concrete => concrete,
                IEnumerable<TElement> enumerable when typeof(TConcreteCollection) == typeof(TElement[]) => 
                    (TConcreteCollection)(object)enumerable.ToArray(),
                IEnumerable<TElement> enumerable when typeof(TConcreteCollection).IsAssignableFrom(typeof(List<TElement>)) => 
                    (TConcreteCollection)(object)enumerable.ToList(),
                _ => throw new InvalidOperationException($"Cannot convert {value?.GetType()} to {typeof(TConcreteCollection)}")
            };
        }

        private static TCollection ConvertToCollection(TConcreteCollection value)
        {
            if (typeof(TCollection).IsAssignableFrom(typeof(TConcreteCollection)))
                return (TCollection)(object)value;

            if (typeof(TCollection).IsInterface && value is IEnumerable<TElement>)
                return (TCollection)(object)value;

            throw new InvalidOperationException($"Cannot convert {value?.GetType()} to {typeof(TCollection)}");
        }

        private static ValueComparer? CreateValueComparer(ValueComparer elementComparer)
        {
            return new ValueComparer<TConcreteCollection>(
                (l, r) => Compare(l, r, elementComparer),
                v => GetHashCode(v, elementComparer),
                v => Snapshot(v));
        }

        private static bool Compare(TConcreteCollection? left, TConcreteCollection? right, ValueComparer elementComparer)
        {
            if (ReferenceEquals(left, right)) return true;
            if (left is null || right is null) return false;
            
            var leftArray = left.ToArray();
            var rightArray = right.ToArray();
            
            if (leftArray.Length != rightArray.Length) return false;
            
            for (var i = 0; i < leftArray.Length; i++)
            {
                if (!elementComparer.Equals(leftArray[i], rightArray[i]))
                    return false;
            }
            
            return true;
        }

        private static int GetHashCode(TConcreteCollection obj, ValueComparer elementComparer)
        {
            var hash = new HashCode();
            foreach (var element in obj)
            {
                hash.Add(elementComparer.GetHashCode(element));
            }
            return hash.ToHashCode();
        }

        private static TConcreteCollection Snapshot(TConcreteCollection source)
        {
            if (typeof(TConcreteCollection) == typeof(TElement[]))
                return (TConcreteCollection)(object)source.ToArray();
            
            if (typeof(TConcreteCollection).IsAssignableFrom(typeof(List<TElement>)))
                return (TConcreteCollection)(object)source.ToList();
            
            return source; // For cases where TConcreteCollection is the same as TCollection
        }

        protected BigQueryArrayTypeMapping(RelationalTypeMappingParameters parameters)
            : base(parameters)
        {
        }

        // This constructor exists only to support the static Default property
        private BigQueryArrayTypeMapping()
            : base(new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(typeof(TCollection)),
                "ARRAY<INT64>"))
        {
        }

        protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        {
            return new BigQueryArrayTypeMapping<TCollection, TConcreteCollection, TElement>(parameters);
        }

        protected override string GenerateNonNullSqlLiteral(object value)
        {
            if (value is not IEnumerable enumerable)
                throw new ArgumentException($"'{value.GetType().Name}' must be an IEnumerable", nameof(value));

            if (value is Array array && array.Rank != 1)
                throw new NotSupportedException("Multidimensional array literals aren't supported");

            var sb = new StringBuilder();

            sb.Append("ARRAY<");
            sb.Append(ElementTypeMapping.StoreType);
            sb.Append(">[");

            var isFirst = true;
            foreach (var element in enumerable)
            {
                if (isFirst)
                    isFirst = false;
                else
                    sb.Append(", ");

                sb.Append(ElementTypeMapping.GenerateProviderValueSqlLiteral(element));
            }

            sb.Append(']');
            return sb.ToString();
        }

        protected override void ConfigureParameter(DbParameter parameter)
        {
            base.ConfigureParameter(parameter);

            if (parameter is Data.BigQuery.BigQueryParameter bqParameter)
            {
                bqParameter.BigQueryDbType = Google.Cloud.BigQuery.V2.BigQueryDbType.Array;
            }
        }
    }
}