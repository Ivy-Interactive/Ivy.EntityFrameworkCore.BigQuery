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

            return new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(
                    clrType,
                    converter: null, // Conversion happens in DataReader.
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
