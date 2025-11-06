using Google.Cloud.BigQuery.V2;
using Ivy.EntityFrameworkCore.BigQuery.Storage.Internal.Mapping;
using Microsoft.EntityFrameworkCore.Storage;
using System.Collections.Concurrent;

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
            return base.FindMapping(mappingInfo) ?? FindBaseMapping(mappingInfo)?.Clone(mappingInfo);
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
                    return FindMapping(clrType.GetEnumUnderlyingType());
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
            return baseName;
        }
    }
}
