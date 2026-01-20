using Ivy.Data.BigQuery;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using System.Data.Common;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Text.Json;

namespace Ivy.EntityFrameworkCore.BigQuery.Storage.Internal.Mapping
{
    /// <summary>
    /// Type mapping for BigQuery JSON data type.
    /// Supports mapping JSON columns to JsonElement, JsonDocument, string, or user POCOs.
    /// </summary>
    public class BigQueryJsonTypeMapping : RelationalTypeMapping
    {
        /// <summary>
        /// Default JSON type mapping (maps to JsonElement).
        /// </summary>
        public static BigQueryJsonTypeMapping Default { get; } = new("JSON", typeof(JsonElement));

        /// <summary>
        /// Initializes a new instance of the <see cref="BigQueryJsonTypeMapping"/> class.
        /// </summary>
        /// <param name="storeType">The BigQuery store type (should be "JSON").</param>
        /// <param name="clrType">The CLR type to map to (JsonElement, JsonDocument, string, or POCO).</param>
        public BigQueryJsonTypeMapping(string storeType, Type clrType)
            : base(
                new RelationalTypeMappingParameters(
                    new CoreTypeMappingParameters(clrType, converter: CreateConverter(clrType)),
                    storeType))
        {
            if (storeType != "JSON")
            {
                throw new ArgumentException($"{nameof(storeType)} must be 'JSON'", nameof(storeType));
            }
        }

        private static ValueConverter? CreateConverter(Type clrType)
        {
            if (clrType == typeof(JsonDocument))
            {
                return new ValueConverter<JsonDocument, string>(
                    doc => doc.RootElement.ToString(),
                    str => ParseJsonDocument(str));
            }
            else if (clrType == typeof(JsonElement))
            {
                return new ValueConverter<JsonElement, string>(
                    elem => elem.ToString(),
                    str => ParseJsonDocument(str).RootElement);
            }
            else if (clrType == typeof(string))
            {
                return null;
            }
            else
            {
                var converterType = typeof(JsonPocoConverter<>).MakeGenericType(clrType);
                return (ValueConverter)Activator.CreateInstance(converterType)!;
            }
        }

        private static JsonDocument ParseJsonDocument(string json)
            => JsonDocument.Parse(json);

        private class JsonPocoConverter<T> : ValueConverter<T, string>
        {
            public JsonPocoConverter()
                : base(
                    obj => SerializeObject(obj),
                    str => DeserializeObject(str))
            {
            }

            private static string SerializeObject(T obj)
                => JsonSerializer.Serialize(obj);

            private static T DeserializeObject(string str)
                => JsonSerializer.Deserialize<T>(str)!;
        }

        /// <summary>
        /// Initializes a new instance from existing parameters.
        /// </summary>
        protected BigQueryJsonTypeMapping(RelationalTypeMappingParameters parameters)
            : base(parameters)
        {
        }

        /// <summary>
        /// Creates a copy of this mapping with new parameters.
        /// </summary>
        protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
            => new BigQueryJsonTypeMapping(parameters);

        /// <summary>
        /// Escapes single quotes in SQL literals by doubling them.
        /// </summary>
        protected virtual string EscapeSqlLiteral(string literal)
            => literal?.Replace("'", "''") ?? string.Empty;

        /// <summary>
        /// Generates a SQL literal for a JSON value using BigQuery's JSON '...' syntax.
        /// </summary>
        protected override string GenerateNonNullSqlLiteral(object value)
        {
            switch (value)
            {
                case JsonDocument doc:
                {
                    using var stream = new MemoryStream();
                    using var writer = new Utf8JsonWriter(stream);
                    doc.WriteTo(writer);
                    writer.Flush();
                    var json = Encoding.UTF8.GetString(stream.ToArray());
                    return $"JSON '{EscapeSqlLiteral(json)}'";
                }
                case JsonElement element:
                {
                    using var stream = new MemoryStream();
                    using var writer = new Utf8JsonWriter(stream);
                    element.WriteTo(writer);
                    writer.Flush();
                    var json = Encoding.UTF8.GetString(stream.ToArray());
                    return $"JSON '{EscapeSqlLiteral(json)}'";
                }
                case string s:
                    return $"JSON '{EscapeSqlLiteral(s)}'";
                default:
                    var jsonString = JsonSerializer.Serialize(value);
                    return $"JSON '{EscapeSqlLiteral(jsonString)}'";
            }
        }

        /// <summary>
        /// Generates code literal for design-time scaffolding.
        /// </summary>
        public override Expression GenerateCodeLiteral(object value)
            => value switch
            {
                JsonDocument document => Expression.Call(
                    ParseMethod,
                    Expression.Constant(document.RootElement.ToString()),
                    DefaultJsonDocumentOptions),
                JsonElement element => Expression.Property(
                    Expression.Call(
                        ParseMethod,
                        Expression.Constant(element.ToString()),
                        DefaultJsonDocumentOptions),
                    nameof(JsonDocument.RootElement)),
                string s => Expression.Constant(s),
                _ => throw new NotSupportedException("Cannot generate code literals for JSON POCOs")
            };

        private static readonly Expression DefaultJsonDocumentOptions = Expression.New(typeof(JsonDocumentOptions));

        private static readonly MethodInfo ParseMethod =
            typeof(JsonDocument).GetMethod(nameof(JsonDocument.Parse), new[] { typeof(string), typeof(JsonDocumentOptions) })!;

        /// <summary>
        /// Configures a database parameter with JSON-specific settings.
        /// </summary>
        protected override void ConfigureParameter(DbParameter parameter)
        {
            base.ConfigureParameter(parameter);

            if (parameter is BigQueryParameter bqParam)
            {
                if (parameter.Value != null && parameter.Value != DBNull.Value)
                {
                    parameter.Value = ConvertToJsonString(parameter.Value);
                }

                bqParam.BigQueryDbType = Google.Cloud.BigQuery.V2.BigQueryDbType.Json;
            }
        }

        /// <summary>
        /// Creates a parameter. Overridden to handle string values directly since EF Core's owned JSON
        /// infrastructure passes serialized JSON strings, bypassing the value converter sanitization.
        /// </summary>
        public override DbParameter CreateParameter(DbCommand command, string name, object? value, bool? nullable, System.Data.ParameterDirection direction)
        {
            var parameter = command.CreateParameter();
            parameter.Direction = direction;
            parameter.ParameterName = name;

            // Handle values directly without going through value converter sanitization
            if (value is string stringValue)
            {
                parameter.Value = stringValue;
            }
            else if (value is JsonElement jsonElement)
            {
                parameter.Value = jsonElement.GetRawText();
            }
            else if (value is JsonDocument jsonDocument)
            {
                parameter.Value = jsonDocument.RootElement.GetRawText();
            }
            else if (value == null || value == DBNull.Value)
            {
                parameter.Value = DBNull.Value;
            }
            else
            {
                parameter.Value = JsonSerializer.Serialize(value);
            }

            if (nullable.HasValue)
            {
                parameter.IsNullable = nullable.Value;
            }

            ConfigureParameter(parameter);
            return parameter;
        }

        /// <summary>
        /// Converts a value to a JSON string for parameter binding.
        /// </summary>
        private string ConvertToJsonString(object value)
        {
            return value switch
            {
                JsonDocument doc => doc.RootElement.ToString(),
                JsonElement element => element.ToString(),
                string s => s,
                _ => JsonSerializer.Serialize(value)
            };
        }
    }
}