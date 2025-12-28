using Google.Cloud.BigQuery.V2;
using Ivy.EntityFrameworkCore.BigQuery.Storage.Internal.Mapping;
using Microsoft.EntityFrameworkCore.Storage;
using System.Collections.Concurrent;
using System.Text;

namespace Ivy.EntityFrameworkCore.BigQuery.Storage.Internal
{
    public class BigQueryTypeMappingSource : RelationalTypeMappingSource
    {
        private readonly BigQueryStringTypeMapping _string = new();
        private readonly BigQueryByteArrayTypeMapping _bytes = new();
        private readonly BigQueryBoolTypeMapping _bool = new();
        private readonly BigQueryInt64TypeMapping _long = new();
        private readonly BigQueryDoubleTypeMapping _double = new();
        private readonly BigQueryDateTimeOffsetTypeMapping _timestamp = new();
        private readonly BigQueryDateTimeTypeMapping _dateTime = new();
        private readonly BigQueryDateOnlyTypeMapping _date = new();
        private readonly BigQueryTimeOnlyTypeMapping _time = new();
        private readonly BigQueryDecimalTypeMapping _decimal = new(); // BIGNUMERIC(57, 28)
        private readonly BigQueryNumericTypeMapping _numericDefault = new("NUMERIC");
        private readonly BigQueryBigNumericTypeMapping _bigNumericDefault = new("BIGNUMERIC");
        private readonly BigQueryGuidTypeMapping _guid = new();


        private readonly BigQueryFloatTypeMapping _float = new();
        private readonly BigQueryIntTypeMapping _int = new();
        private readonly BigQueryShortTypeMapping _short = new();
        private readonly BigQueryByteTypeMapping _byte = new();

        private readonly ConcurrentDictionary<string, RelationalTypeMapping> _storeTypeMappings;
        private readonly ConcurrentDictionary<Type, RelationalTypeMapping> _clrTypeMappings;

        public BigQueryTypeMappingSource(
            TypeMappingSourceDependencies dependencies,
            RelationalTypeMappingSourceDependencies relationalDependencies)
            : base(dependencies, relationalDependencies)
        {

            var storeTypeMappings = new Dictionary<string, List<RelationalTypeMapping>>(StringComparer.OrdinalIgnoreCase)
            {
                { "STRING", new List<RelationalTypeMapping> { _string, _guid } },
                { "BYTES", new List<RelationalTypeMapping> { _bytes } },
                { "BOOL", new List<RelationalTypeMapping> { _bool } },
                { "INT64", new List<RelationalTypeMapping> { _long, _int, _short, _byte } },
                { "INTEGER", new List<RelationalTypeMapping> { _long, _int, _short, _byte } },
                { "FLOAT64", new List<RelationalTypeMapping> { _double, _float } },
                { "FLOAT", new List<RelationalTypeMapping> { _double, _float } },
                { "TIMESTAMP", new List<RelationalTypeMapping> { _timestamp } },
                { "DATETIME", new List<RelationalTypeMapping> { _dateTime } },
                { "DATE", new List<RelationalTypeMapping> { _date } },
                { "TIME", new List<RelationalTypeMapping> { _time } },
                { "BIGNUMERIC", new List<RelationalTypeMapping> { _bigNumericDefault } },
                { "NUMERIC", new List<RelationalTypeMapping> { _numericDefault } },
                { "BIGNUMERIC(57, 28)", new List<RelationalTypeMapping> { _decimal } },
            };

            _storeTypeMappings = new ConcurrentDictionary<string, RelationalTypeMapping>(
                storeTypeMappings.ToDictionary(kvp => kvp.Key, kvp => kvp.Value.First()),
                StringComparer.OrdinalIgnoreCase);

            var clrTypeMappings = new Dictionary<Type, RelationalTypeMapping>
            {
                { typeof(string), _string },
                { typeof(byte[]), _bytes },
                { typeof(bool), _bool },
                { typeof(bool?), _bool },
                { typeof(long), _long },
                { typeof(long?), _long },
                { typeof(double), _double },
                { typeof(double?), _double },
                { typeof(DateTimeOffset), _timestamp },
                { typeof(DateTimeOffset?), _timestamp },
                { typeof(DateTime), _dateTime },
                { typeof(DateTime?), _dateTime },
                { typeof(DateOnly), _date },
                { typeof(DateOnly?), _date },
                { typeof(TimeOnly), _time },
                { typeof(TimeOnly?), _time },
                { typeof(decimal), _decimal },
                { typeof(decimal?), _decimal },
                { typeof(Guid), _guid },
                { typeof(Guid?), _guid },
                { typeof(BigQueryNumeric), _numericDefault },
                { typeof(BigQueryNumeric?), _numericDefault },
                { typeof(BigQueryBigNumeric), _bigNumericDefault },
                { typeof(BigQueryBigNumeric?), _bigNumericDefault },

                { typeof(float), _float },
                { typeof(float?), _float },
                { typeof(int), _int },
                { typeof(int?), _int },
                { typeof(short), _short },
                { typeof(short?), _short },
                { typeof(byte), _byte },
                { typeof(byte?), _byte },
            };
            _clrTypeMappings = new ConcurrentDictionary<Type, RelationalTypeMapping>(clrTypeMappings);
        }

        protected override RelationalTypeMapping? FindMapping(in RelationalTypeMappingInfo mappingInfo)
        {
            var structMapping = FindStructMapping(mappingInfo);
            if (structMapping != null)
            {
                return structMapping;
            }

            return base.FindMapping(mappingInfo) ?? FindBaseMapping(mappingInfo)?.Clone(mappingInfo);
        }

        /// <summary>
        /// Find mapping for STRUCT types (both from store type string and CLR types with [BigQueryStruct] attribute)
        /// </summary>
        protected virtual RelationalTypeMapping? FindStructMapping(in RelationalTypeMappingInfo mappingInfo)
        {
            var storeTypeName = mappingInfo.StoreTypeName;
            var clrType = mappingInfo.ClrType;

            if (storeTypeName != null && storeTypeName.StartsWith("STRUCT<", StringComparison.OrdinalIgnoreCase))
            {
                var fields = ParseStructType(storeTypeName);
                if (fields.Count > 0)
                {
                    var type = clrType ?? typeof(IDictionary<string, object>);
                    return new BigQueryStructTypeMapping(type, fields, storeTypeName);
                }
            }

            if (clrType != null && IsOwnedEntityType(clrType))
            {
                var fields = ExtractFieldsFromClrType(clrType);
                if (fields.Count > 0)
                {
                    return new BigQueryStructTypeMapping(clrType, fields);
                }
            }

            if (mappingInfo.ElementTypeMapping != null && clrType != null)
            {
                var fields = ExtractFieldsFromClrType(clrType);
                if (fields.Count > 0)
                {
                    return new BigQueryStructTypeMapping(clrType, fields);
                }
            }

            return null;
        }

        /// <summary>
        /// Parse STRUCT&lt;field1 TYPE1, field2 TYPE2&gt; syntax
        /// </summary>
        protected virtual IReadOnlyList<BigQueryStructTypeMapping.StructFieldInfo> ParseStructType(string storeType)
        {
            if (!storeType.StartsWith("STRUCT<", StringComparison.OrdinalIgnoreCase))
            {
                return Array.Empty<BigQueryStructTypeMapping.StructFieldInfo>();
            }

            var fields = new List<BigQueryStructTypeMapping.StructFieldInfo>();

            var startIndex = "STRUCT<".Length;
            var endIndex = FindMatchingCloseBracket(storeType, startIndex - 1);

            if (endIndex == -1)
            {
                return Array.Empty<BigQueryStructTypeMapping.StructFieldInfo>();
            }

            var fieldsContent = storeType.Substring(startIndex, endIndex - startIndex);

            var fieldDefinitions = SplitStructFields(fieldsContent);

            foreach (var fieldDef in fieldDefinitions)
            {
                var parts = fieldDef.Trim().Split(new[] { ' ' }, 2, StringSplitOptions.RemoveEmptyEntries);
                if (parts.Length != 2)
                {
                    continue;
                }

                var fieldName = parts[0];
                var fieldType = parts[1];

                var fieldTypeMapping = FindMapping(fieldType);
                if (fieldTypeMapping == null)
                {
                    continue;
                }

                fields.Add(new BigQueryStructTypeMapping.StructFieldInfo(
                    fieldName,
                    fieldTypeMapping,
                    fieldTypeMapping.ClrType));
            }

            return fields;
        }

        private static List<string> SplitStructFields(string fieldsContent)
        {
            var fields = new List<string>();
            var currentField = new StringBuilder();
            var depth = 0;

            for (var i = 0; i < fieldsContent.Length; i++)
            {
                var ch = fieldsContent[i];

                if (ch == '<')
                {
                    depth++;
                    currentField.Append(ch);
                }
                else if (ch == '>')
                {
                    depth--;
                    currentField.Append(ch);
                }
                else if (ch == ',' && depth == 0)
                {
                    // Field separator at top level
                    fields.Add(currentField.ToString());
                    currentField.Clear();
                }
                else
                {
                    currentField.Append(ch);
                }
            }

            if (currentField.Length > 0)
            {
                fields.Add(currentField.ToString());
            }

            return fields;
        }

        /// <summary>
        /// Find matching close bracket, handling nested &lt;&gt;
        /// </summary>
        private static int FindMatchingCloseBracket(string str, int openIndex)
        {
            var depth = 1;
            for (var i = openIndex + 1; i < str.Length; i++)
            {
                if (str[i] == '<')
                {
                    depth++;
                }
                else if (str[i] == '>')
                {
                    depth--;
                    if (depth == 0)
                    {
                        return i;
                    }
                }
            }
            return -1;
        }

        private static bool IsOwnedEntityType(Type type)
        {
            // Exclude primitive and common types
            if (type.IsPrimitive || type == typeof(string) || type == typeof(decimal) ||
                type == typeof(DateTime) || type == typeof(DateTimeOffset) ||
                type == typeof(Guid) || type == typeof(byte[]))
            {
                return false;
            }

            // Check if it has [Owned] attribute (for compatibility)
            var ownedAttr = type.GetCustomAttributes(typeof(Microsoft.EntityFrameworkCore.OwnedAttribute), true);
            if (ownedAttr.Length > 0)
            {
                return true;
            }

            var structAttr = type.GetCustomAttributes(typeof(Metadata.BigQueryStructAttribute), true);
            return structAttr.Length > 0;
        }

        private IReadOnlyList<BigQueryStructTypeMapping.StructFieldInfo> ExtractFieldsFromClrType(Type clrType)
        {
            var fields = new List<BigQueryStructTypeMapping.StructFieldInfo>();

            var properties = clrType.GetProperties(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance);

            foreach (var property in properties)
            {
                // Navigation properties
                if (!property.CanRead || !property.CanWrite)
                {
                    continue;
                }

                var propertyTypeMapping = FindMapping(property.PropertyType);
                if (propertyTypeMapping == null)
                {
                    continue;
                }

                // [Column]
                var fieldName = GetColumnName(property);

                fields.Add(new BigQueryStructTypeMapping.StructFieldInfo(
                    fieldName,
                    propertyTypeMapping,
                    property.PropertyType,
                    property.Name));
            }

            return fields;
        }

        private static string GetColumnName(System.Reflection.PropertyInfo property)
        {
            // [Column("custom_name")]
            var columnAttr = property.GetCustomAttributes(typeof(System.ComponentModel.DataAnnotations.Schema.ColumnAttribute), true)
                .FirstOrDefault() as System.ComponentModel.DataAnnotations.Schema.ColumnAttribute;

            return columnAttr?.Name ?? property.Name;
        }

        protected virtual RelationalTypeMapping? FindBaseMapping(in RelationalTypeMappingInfo mappingInfo)
        {
            var clrType = mappingInfo.ClrType;
            var storeTypeName = mappingInfo.StoreTypeName;

            if (storeTypeName != null)
            {
                if (_storeTypeMappings.TryGetValue(storeTypeName, out var mapping))
                {
                    return clrType != null && mapping.ClrType != clrType
                        ? FindMapping(clrType)
                        : mapping;
                }

                var storeTypeNameBase = GetStoreTypeBaseName(storeTypeName);
                if (_storeTypeMappings.TryGetValue(storeTypeNameBase, out mapping))
                {
                    return mapping;
                }
            }

            if (clrType != null)
            {
                if (_clrTypeMappings.TryGetValue(clrType, out var mapping))
                {
                    return mapping;
                }

                if (clrType.IsEnum)
                {
                    var underlyingType = clrType.GetEnumUnderlyingType();
                    var underlyingTypeMapping = FindMapping(underlyingType);

                    if (underlyingTypeMapping != null)
                    {
                        var converterType = typeof(Microsoft.EntityFrameworkCore.Storage.ValueConversion.EnumToNumberConverter<,>)
                            .MakeGenericType(clrType, underlyingType);
                        var converter = (Microsoft.EntityFrameworkCore.Storage.ValueConversion.ValueConverter)
                            Activator.CreateInstance(converterType)!;

                        return (RelationalTypeMapping)underlyingTypeMapping.WithComposedConverter(converter);
                    }
                }
            }

            return null;
        }

        private string GetStoreTypeBaseName(string storeTypeName)
        {
            var openParen = storeTypeName.IndexOf('(');
            return openParen == -1 ? storeTypeName : storeTypeName.Substring(0, openParen);
        }

        protected override string? ParseStoreTypeName(string? storeTypeName, ref bool? unicode, ref int? size, ref int? precision, ref int? scale)
        {
            if (storeTypeName == null)
            {
                return null;
            }

            var baseName = GetStoreTypeBaseName(storeTypeName);
            var openParen = storeTypeName.IndexOf('(');

            if (openParen > 0)
            {
                var closeParen = storeTypeName.LastIndexOf(')');
                if (closeParen > openParen)
                {
                    var facets = storeTypeName.Substring(openParen + 1, closeParen - openParen - 1).Split(',');

                    if (baseName.Equals("BIGNUMERIC", StringComparison.OrdinalIgnoreCase) ||
                        baseName.Equals("NUMERIC", StringComparison.OrdinalIgnoreCase))
                    {
                        if (facets.Length > 0 && int.TryParse(facets[0], out var p))
                        {
                            precision = p;
                        }
                        if (facets.Length > 1 && int.TryParse(facets[1], out var s))
                        {
                            scale = s;
                        }
                    }
                }
            }
            return baseName;
        }
    }
}
