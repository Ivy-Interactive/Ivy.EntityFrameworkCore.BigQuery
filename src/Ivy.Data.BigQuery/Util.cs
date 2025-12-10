using Google.Cloud.BigQuery.V2;
using static System.Runtime.InteropServices.JavaScript.JSType;

namespace Ivy.Data.BigQuery;

public static class Util
{
    private static readonly Dictionary<BigQueryDbType, string> ParameterApiToDbType = new()
    {
        { BigQueryDbType.Int64, "INTEGER" },
        { BigQueryDbType.Float64, "FLOAT" },
        { BigQueryDbType.Bool, "BOOL" },
        { BigQueryDbType.String, "STRING" },
        { BigQueryDbType.Bytes, "BYTES" },
        { BigQueryDbType.Date, "DATE" },
        { BigQueryDbType.DateTime, "DATETIME" },
        { BigQueryDbType.Time, "TIME" },
        { BigQueryDbType.Timestamp, "TIMESTAMP" },
        { BigQueryDbType.Array, "ARRAY" },
        { BigQueryDbType.Struct, "STRUCT" },
        { BigQueryDbType.Numeric, "NUMERIC" },
        { BigQueryDbType.Geography, "GEOGRAPHY" },
        { BigQueryDbType.BigNumeric, "BIGNUMERIC" },
        { BigQueryDbType.Json, "JSON" }
    };

    private static readonly Dictionary<string, BigQueryDbType> _nameToTypeMapping = new()
    {
        { "INTEGER", BigQueryDbType.Int64 },
        { "INT", BigQueryDbType.Int64 },
        { "SMALLINT", BigQueryDbType.Int64 },
        { "BIGINT", BigQueryDbType.Int64 },
        { "TINYINT", BigQueryDbType.Int64 },
        { "BYTEINT", BigQueryDbType.Int64 },
        { "FLOAT", BigQueryDbType.Float64 },
        { "BOOL", BigQueryDbType.Bool },
        { "BOOLEAN", BigQueryDbType.Bool },
        { "STRING", BigQueryDbType.String },
        { "BYTES", BigQueryDbType.Bytes },
        { "DATE", BigQueryDbType.Date },
        { "DATETIME", BigQueryDbType.DateTime },
        { "TIME", BigQueryDbType.Time },
        { "TIMESTAMP", BigQueryDbType.Timestamp },
        { "ARRAY", BigQueryDbType.Array },
        { "STRUCT", BigQueryDbType.Struct },
        { "RECORD", BigQueryDbType.Struct },
        { "NUMERIC", BigQueryDbType.Numeric },
        { "DECIMAL", BigQueryDbType.Numeric },
        { "GEOGRAPHY", BigQueryDbType.Geography },
        { "BIGNUMERIC", BigQueryDbType.BigNumeric },
        { "BIGDECIMAL", BigQueryDbType.BigNumeric },
        { "JSON", BigQueryDbType.Json }
    };

    public static readonly HashSet<BigQueryDbType> NumericTypes = new()
    {
        BigQueryDbType.Int64,
        BigQueryDbType.Float64,
        BigQueryDbType.Numeric,
        BigQueryDbType.BigNumeric
    };

    public static string DbTypeToParameterApiType(BigQueryDbType type) => ParameterApiToDbType[type];

    public static BigQueryDbType ParameterApiTypeToDbType(string typeName)
    {
        if (_nameToTypeMapping.TryGetValue(typeName, out var type))
        {
            return type;
        }
        throw new ArgumentException($"Unknown BigQuery type: {typeName}");
    }

    public static bool IsNumericType(BigQueryDbType type)
    {
        return NumericTypes.Contains(type);
    }
}
