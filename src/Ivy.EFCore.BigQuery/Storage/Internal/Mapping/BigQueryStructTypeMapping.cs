using Google.Cloud.BigQuery.V2;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Storage;
using System.Data.Common;
using System.Reflection;
using System.Text;

namespace Ivy.EntityFrameworkCore.BigQuery.Storage.Internal.Mapping
{
    public class BigQueryStructTypeMapping : RelationalTypeMapping
    {
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
            var comparer = CreateValueComparer(clrType, fields);
            var converter = CreateValueConverter(clrType, fields);

            return new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(
                    clrType,
                    converter: converter,
                    comparer: comparer),
                storeType);
        }

        private static Microsoft.EntityFrameworkCore.Storage.ValueConversion.ValueConverter? CreateValueConverter(
            Type clrType,
            IReadOnlyList<StructFieldInfo> fields)
        {
            // Create a converter that converts CLR objects to/from Dictionary<string, object>
            // This is needed for parameter binding (CLR -> Dict) and reading (Dict -> CLR)
            var converterType = typeof(StructValueConverter<>).MakeGenericType(clrType);
            return (Microsoft.EntityFrameworkCore.Storage.ValueConversion.ValueConverter?)
                Activator.CreateInstance(converterType, fields);
        }

        private class StructValueConverter<TStruct> : Microsoft.EntityFrameworkCore.Storage.ValueConversion.ValueConverter<TStruct, IDictionary<string, object?>>
            where TStruct : class
        {
            public StructValueConverter(IReadOnlyList<StructFieldInfo> fields)
                : base(
                    v => ConvertToDict(v, fields),
                    v => ConvertFromDict(v, fields))
            {
            }

            private static IDictionary<string, object?> ConvertToDict(TStruct? value, IReadOnlyList<StructFieldInfo> fields)
            {
                if (value == null)
                    return new Dictionary<string, object?>();

                var dict = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
                var valueType = value.GetType();

                foreach (var field in fields)
                {
                    var propertyName = field.PropertyName ?? field.Name;
                    var property = valueType.GetProperty(propertyName,
                        BindingFlags.IgnoreCase | BindingFlags.Public | BindingFlags.Instance);

                    if (property != null)
                    {
                        var fieldValue = property.GetValue(value);

                        // If the field is a nested struct, recursively convert it
                        if (field.TypeMapping is BigQueryStructTypeMapping nestedStructMapping && fieldValue != null)
                        {
                            fieldValue = ConvertNestedStruct(fieldValue, nestedStructMapping.Fields);
                        }

                        dict[field.Name] = fieldValue;
                    }
                }

                return dict;
            }

            private static IDictionary<string, object?> ConvertNestedStruct(object value, IReadOnlyList<StructFieldInfo> fields)
            {
                var dict = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
                var valueType = value.GetType();

                foreach (var field in fields)
                {
                    var propertyName = field.PropertyName ?? field.Name;
                    var property = valueType.GetProperty(propertyName,
                        BindingFlags.IgnoreCase | BindingFlags.Public | BindingFlags.Instance);

                    if (property != null)
                    {
                        var fieldValue = property.GetValue(value);

                        if (field.TypeMapping is BigQueryStructTypeMapping nestedStructMapping && fieldValue != null)
                        {
                            fieldValue = ConvertNestedStruct(fieldValue, nestedStructMapping.Fields);
                        }

                        dict[field.Name] = fieldValue;
                    }
                }

                return dict;
            }

            private static TStruct ConvertFromDict(IDictionary<string, object?> dict, IReadOnlyList<StructFieldInfo> fields)
            {
                if (dict == null)
                    return default!;

                var instance = Activator.CreateInstance<TStruct>();
                var instanceType = typeof(TStruct);

                foreach (var field in fields)
                {
                    var propertyName = field.PropertyName ?? field.Name;
                    var property = instanceType.GetProperty(propertyName,
                        BindingFlags.IgnoreCase | BindingFlags.Public | BindingFlags.Instance);

                    if (property == null || !property.CanWrite)
                        continue;

                    object? fieldValue = null;
                    foreach (var kvp in dict)
                    {
                        if (string.Equals(kvp.Key, field.Name, StringComparison.OrdinalIgnoreCase))
                        {
                            fieldValue = kvp.Value;
                            break;
                        }
                    }

                    if (fieldValue != null)
                    {
                        // If the field is a nested struct, recursively convert it
                        if (field.TypeMapping is BigQueryStructTypeMapping nestedStructMapping
                            && fieldValue is IDictionary<string, object> nestedDict)
                        {
                            var nestedType = property.PropertyType;
                            fieldValue = ConvertNestedStructFromDict(nestedDict, nestedType, nestedStructMapping.Fields);
                        }

                        property.SetValue(instance, fieldValue);
                    }
                }

                return instance;
            }

            private static object? ConvertNestedStructFromDict(
                IDictionary<string, object> dict,
                Type targetType,
                IReadOnlyList<StructFieldInfo> fields)
            {
                var instance = Activator.CreateInstance(targetType);
                if (instance == null)
                    return null;

                foreach (var field in fields)
                {
                    var propertyName = field.PropertyName ?? field.Name;
                    var property = targetType.GetProperty(propertyName,
                        BindingFlags.IgnoreCase | BindingFlags.Public | BindingFlags.Instance);

                    if (property == null || !property.CanWrite)
                        continue;

                    object? fieldValue = null;
                    foreach (var kvp in dict)
                    {
                        if (string.Equals(kvp.Key, field.Name, StringComparison.OrdinalIgnoreCase))
                        {
                            fieldValue = kvp.Value;
                            break;
                        }
                    }

                    if (fieldValue != null)
                    {
                        if (field.TypeMapping is BigQueryStructTypeMapping nestedStructMapping
                            && fieldValue is IDictionary<string, object> nestedDict)
                        {
                            fieldValue = ConvertNestedStructFromDict(nestedDict, property.PropertyType, nestedStructMapping.Fields);
                        }

                        property.SetValue(instance, fieldValue);
                    }
                }

                return instance;
            }
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
                // Use base type for literal construction (BigQuery doesn't allow parameterized types in literals)
                sb.Append(GetBaseType(field.TypeMapping.StoreType));
            }

            sb.Append(">");
            return sb.ToString();
        }

        /// <summary>
        /// Gets the base type without precision/scale parameters.
        /// BigQuery doesn't allow parameterized types like BIGNUMERIC(57, 28) in literal value construction.
        /// </summary>
        private static string GetBaseType(string storeType)
        {
            if (string.IsNullOrEmpty(storeType))
                return storeType;

            // Handle nested STRUCT or ARRAY types - these are already in the correct format
            if (storeType.StartsWith("STRUCT<", StringComparison.OrdinalIgnoreCase) ||
                storeType.StartsWith("ARRAY<", StringComparison.OrdinalIgnoreCase))
            {
                return storeType;
            }

            // Strip precision/scale from types like BIGNUMERIC(57, 28), NUMERIC(38, 9), STRING(100)
            var parenIndex = storeType.IndexOf('(');
            if (parenIndex > 0)
            {
                return storeType.Substring(0, parenIndex);
            }

            return storeType;
        }

        private static IReadOnlyList<StructFieldInfo> ExtractFieldsFromStoreType(string storeType)
        {
            //todo
            return Array.Empty<StructFieldInfo>();
        }

        protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        {
            return new BigQueryStructTypeMapping(parameters, Fields);
        }

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

                    var propertyName = field.PropertyName ?? field.Name;
                    var property = valueType.GetProperty(propertyName,
                        BindingFlags.IgnoreCase | BindingFlags.Public | BindingFlags.Instance);

                    if (property == null)
                    {
                        throw new InvalidOperationException(
                            $"Property '{propertyName}' not found on type '{valueType.Name}'");
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

        private static ValueComparer? CreateValueComparer(Type structType, IReadOnlyList<StructFieldInfo> fields)
        {
            return (ValueComparer?)Activator.CreateInstance(
                typeof(StructValueComparer<>).MakeGenericType(structType),
                fields);
        }

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
