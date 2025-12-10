using Google.Cloud.BigQuery.V2;
using Ivy.Data.BigQuery;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using System.Collections;
using System.Data.Common;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace Ivy.EntityFrameworkCore.BigQuery.Storage.Internal.Mapping
{
    /// <summary>
    /// Type mapping for BigQuery STRUCT types.
    /// Uses value converters to convert between IDictionary&lt;string, object&gt; (ADO.NET) and CLR types.
    /// STRUCTs are treated as complex value types, not owned entities.
    /// </summary>
    public class BigQueryStructTypeMapping : RelationalTypeMapping
    {
        /// <summary>
        /// Information about a single field in a struct
        /// </summary>
        public class StructFieldInfo
        {
            public string Name { get; }
            public RelationalTypeMapping TypeMapping { get; }
            public Type Type { get; }
            public string? PropertyName { get; }

            public StructFieldInfo(string name, RelationalTypeMapping typeMapping, Type type, string? propertyName = null)
            {
                Name = name;
                TypeMapping = typeMapping;
                Type = type;
                PropertyName = propertyName;
            }
        }

        /// <summary>
        /// The field definitions for this struct
        /// </summary>
        public virtual IReadOnlyList<StructFieldInfo> Fields { get; }

        public BigQueryStructTypeMapping(
            Type clrType,
            IReadOnlyList<StructFieldInfo> fields,
            string? storeType = null)
            : this(CreateParameters(clrType, fields, storeType ?? BuildStoreType(fields)), fields)
        {
        }

        private static RelationalTypeMappingParameters CreateParameters(
            Type clrType,
            IReadOnlyList<StructFieldInfo> fields,
            string storeType)
        {
            // For struct change tracking
            var comparer = CreateValueComparer(clrType, fields);

            // IDictionary<string, object> <-> CLR struct type
            var converter = CreateValueConverter(clrType, fields);

            return new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(
                    clrType,
                    converter: converter,
                    comparer: comparer),
                storeType);
        }

        protected BigQueryStructTypeMapping(RelationalTypeMappingParameters parameters)
            : base(parameters)
        {
            Fields = ExtractFieldsFromStoreType(parameters.StoreType);
        }

        private BigQueryStructTypeMapping(
            RelationalTypeMappingParameters parameters,
            IReadOnlyList<StructFieldInfo> fields)
            : base(parameters)
        {
            Fields = fields;
        }

        /// <summary>
        /// Build store type string from field definitions: STRUCT&lt;field1 TYPE1, field2 TYPE2&gt;
        /// </summary>
        private static string BuildStoreType(IReadOnlyList<StructFieldInfo> fields)
        {
            var sb = new StringBuilder("STRUCT<");

            for (var i = 0; i < fields.Count; i++)
            {
                if (i > 0)
                {
                    sb.Append(", ");
                }

                var field = fields[i];
                sb.Append(field.Name);
                sb.Append(" ");
                sb.Append(field.TypeMapping.StoreType);
            }

            sb.Append(">");
            return sb.ToString();
        }

        /// <summary>
        /// Extract field information from store type string (for scaffolding)
        /// </summary>
        private static IReadOnlyList<StructFieldInfo> ExtractFieldsFromStoreType(string storeType)
        {
            // This will be properly implemented when we add parsing in BigQueryTypeMappingSource
            // For now, return empty list
            return Array.Empty<StructFieldInfo>();
        }

        protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        {
            return new BigQueryStructTypeMapping(parameters, Fields);
        }

        /// <summary>
        /// Generate SQL literal: STRUCT&lt;name STRING, age INT64&gt;('John', 25)
        /// </summary>
        protected override string GenerateNonNullSqlLiteral(object value)
        {
            var sb = new StringBuilder();

            sb.Append(Parameters.StoreType);
            sb.Append("(");

            if (value is IDictionary<string, object> dict)
            {
                for (var i = 0; i < Fields.Count; i++)
                {
                    if (i > 0)
                    {
                        sb.Append(", ");
                    }

                    var field = Fields[i];

                    object? fieldValue = null;
                    foreach (var kvp in dict)
                    {
                        if (string.Equals(kvp.Key, field.Name, StringComparison.OrdinalIgnoreCase))
                        {
                            fieldValue = kvp.Value;
                            break;
                        }
                    }

                    if (fieldValue == null)
                    {
                        sb.Append("NULL");
                    }
                    else
                    {
                        sb.Append(field.TypeMapping.GenerateProviderValueSqlLiteral(fieldValue));
                    }
                }
            }
            else
            {
                var valueType = value.GetType();
                for (var i = 0; i < Fields.Count; i++)
                {
                    if (i > 0)
                    {
                        sb.Append(", ");
                    }

                    var field = Fields[i];

                    var property = valueType.GetProperty(field.Name,
                        BindingFlags.IgnoreCase | BindingFlags.Public | BindingFlags.Instance);

                    if (property == null)
                    {
                        throw new InvalidOperationException(
                            $"Property '{field.Name}' not found on type '{valueType.Name}'");
                    }

                    var fieldValue = property.GetValue(value);

                    if (fieldValue == null)
                    {
                        sb.Append("NULL");
                    }
                    else
                    {
                        sb.Append(field.TypeMapping.GenerateProviderValueSqlLiteral(fieldValue));
                    }
                }
            }

            sb.Append(")");
            return sb.ToString();
        }

        protected override void ConfigureParameter(DbParameter parameter)
        {
            base.ConfigureParameter(parameter);

            if (parameter is Ivy.Data.BigQuery.BigQueryParameter bigQueryParameter)
            {
                bigQueryParameter.BigQueryDbType = BigQueryDbType.Struct;
            }
        }

        /// <summary>
        /// Create value converter: IDictionary&lt;string, object&gt; &lt;-&gt; CLR struct type
        /// This converts between the ADO.NET representation and the CLR type
        /// </summary>
        private static ValueConverter? CreateValueConverter(Type structType, IReadOnlyList<StructFieldInfo> fields)
        {
            // this happens during scaffolding or when no CLR type is configured
            if (structType == typeof(IDictionary<string, object>) ||
                structType.IsInterface ||
                !structType.IsClass ||
                structType.IsAbstract)
            {
                return null;
            }

            // Check if type has parameterless constructor (required by TStruct : new() constraint)
            var constructor = structType.GetConstructor(Type.EmptyTypes);
            if (constructor == null)
            {
                return null;
            }

            var converterType = typeof(StructValueConverter<>).MakeGenericType(structType);
            return (ValueConverter?)Activator.CreateInstance(converterType, fields);
        }

        /// <summary>
        /// IDictionary&lt;string, object&gt; <-> CLR struct type
        /// </summary>
        private class StructValueConverter<TStruct> : ValueConverter<TStruct, IDictionary<string, object>>
            where TStruct : new()
        {
            public StructValueConverter(IReadOnlyList<StructFieldInfo> fields)
                : base(
                    v => ConvertStructToDictionary(v, fields),
                    v => ConvertDictionaryToStruct(v, fields))
            {
            }

            private static TStruct ConvertDictionaryToStruct(
                IDictionary<string, object>? dict,
                IReadOnlyList<StructFieldInfo> fields)
            {
                if (dict == null)
                {
                    return default!;
                }

                var instance = new TStruct();
                var instanceType = typeof(TStruct);

                foreach (var field in fields)
                {
                    // [Column]
                    var propertyName = field.PropertyName ?? field.Name;
                    var property = field.PropertyName != null
                        ? instanceType.GetProperty(propertyName, BindingFlags.Public | BindingFlags.Instance)
                        : instanceType.GetProperty(field.Name, BindingFlags.IgnoreCase | BindingFlags.Public | BindingFlags.Instance);

                    if (property == null || !property.CanWrite)
                    {
                        continue;
                    }

                    object? value = null;
                    var foundValue = false;

                    foreach (var kvp in dict)
                    {
                        if (string.Equals(kvp.Key, field.Name, StringComparison.OrdinalIgnoreCase))
                        {
                            value = kvp.Value;
                            foundValue = true;
                            break;
                        }
                    }

                    if (foundValue)
                    {
                        // Nested conversions if the field has a converter
                        if (value != null && field.TypeMapping.Converter != null)
                        {
                            value = field.TypeMapping.Converter.ConvertFromProvider(value);
                        }

                        if (value != null && value.GetType() != property.PropertyType)
                        {
                            var propertyType = property.PropertyType;
                            var underlyingType = Nullable.GetUnderlyingType(propertyType) ?? propertyType;

                            if (underlyingType != value.GetType())
                            {
                                try
                                {
                                    value = Convert.ChangeType(value, underlyingType);
                                }
                                catch
                                {
                                    continue;
                                }
                            }
                        }

                        property.SetValue(instance, value);
                    }
                }

                return instance;
            }

            private static IDictionary<string, object> ConvertStructToDictionary(
                TStruct? structValue,
                IReadOnlyList<StructFieldInfo> fields)
            {
                var dict = new Dictionary<string, object>();

                if (structValue == null)
                {
                    return dict;
                }

                var structType = typeof(TStruct);

                foreach (var field in fields)
                {
                    // [Column]
                    var propertyName = field.PropertyName ?? field.Name;
                    var property = field.PropertyName != null
                        ? structType.GetProperty(propertyName, BindingFlags.Public | BindingFlags.Instance)
                        : structType.GetProperty(field.Name, BindingFlags.IgnoreCase | BindingFlags.Public | BindingFlags.Instance);

                    if (property == null || !property.CanRead)
                    {
                        continue;
                    }

                    var value = property.GetValue(structValue);

                    if (value != null && field.TypeMapping.Converter != null)
                    {
                        value = field.TypeMapping.Converter.ConvertToProvider(value);
                    }

                    if (value != null)
                    {
                        dict[field.Name] = value;
                    }
                }

                return dict;
            }
        }

        /// <summary>
        /// Create value comparer for struct change tracking
        /// </summary>
        private static ValueComparer? CreateValueComparer(Type structType, IReadOnlyList<StructFieldInfo> fields)
        {
            return (ValueComparer?)Activator.CreateInstance(
                typeof(StructValueComparer<>).MakeGenericType(structType),
                fields);
        }

        /// <summary>
        /// Generic value comparer for struct types
        /// </summary>
        private class StructValueComparer<TStruct> : ValueComparer<TStruct>
        {
            public StructValueComparer(IReadOnlyList<StructFieldInfo> fields)
                : base(
                    (l, r) => Compare(l, r, fields),
                    v => GetHashCode(v, fields),
                    v => Snapshot(v, fields))
            {
            }

            private static bool Compare(TStruct? left, TStruct? right, IReadOnlyList<StructFieldInfo> fields)
            {
                if (ReferenceEquals(left, right))
                {
                    return true;
                }

                if (left is null || right is null)
                {
                    return false;
                }

                var leftType = left.GetType();
                var rightType = right.GetType();

                foreach (var field in fields)
                {
                    var leftProperty = leftType.GetProperty(field.Name);
                    var rightProperty = rightType.GetProperty(field.Name);

                    if (leftProperty == null || rightProperty == null)
                    {
                        continue;
                    }

                    var leftValue = leftProperty.GetValue(left);
                    var rightValue = rightProperty.GetValue(right);

                    if (!field.TypeMapping.Comparer.Equals(leftValue, rightValue))
                    {
                        return false;
                    }
                }

                return true;
            }

            private static int GetHashCode(TStruct obj, IReadOnlyList<StructFieldInfo> fields)
            {
                if (obj is null)
                {
                    return 0;
                }

                var hash = new HashCode();
                var objType = obj.GetType();

                foreach (var field in fields)
                {
                    var property = objType.GetProperty(field.Name);
                    if (property != null)
                    {
                        var value = property.GetValue(obj);
                        hash.Add(field.TypeMapping.Comparer.GetHashCode(value));
                    }
                }

                return hash.ToHashCode();
            }

            private static TStruct Snapshot(TStruct source, IReadOnlyList<StructFieldInfo> fields)
            {
                return source;
            }
        }
    }
}
