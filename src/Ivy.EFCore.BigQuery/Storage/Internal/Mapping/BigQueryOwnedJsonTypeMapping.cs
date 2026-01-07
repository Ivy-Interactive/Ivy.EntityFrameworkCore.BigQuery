using Microsoft.EntityFrameworkCore.Storage;
using System.Data.Common;
using System.Reflection;
using System.Text;
using System.Text.Json;

namespace Ivy.EntityFrameworkCore.BigQuery.Storage.Internal.Mapping
{
    /// <summary>
    /// Type mapping for JSON columns in BigQuery
    /// </summary>
    public class BigQueryOwnedJsonTypeMapping : JsonTypeMapping
    {
        private static readonly MethodInfo GetStringMethod
            = typeof(DbDataReader).GetRuntimeMethod(nameof(DbDataReader.GetString), [typeof(int)])!;

        private static readonly PropertyInfo UTF8Property
            = typeof(Encoding).GetProperty(nameof(Encoding.UTF8))!;

        private static readonly MethodInfo EncodingGetBytesMethod
            = typeof(Encoding).GetMethod(nameof(Encoding.GetBytes), [typeof(string)])!;

        private static readonly ConstructorInfo MemoryStreamConstructor
            = typeof(MemoryStream).GetConstructor([typeof(byte[])])!;

        public static BigQueryOwnedJsonTypeMapping Default { get; } = new("JSON");

        public BigQueryOwnedJsonTypeMapping(string storeType)
            : base(storeType, typeof(JsonElement), dbType: null)
        {
        }

        protected BigQueryOwnedJsonTypeMapping(RelationalTypeMappingParameters parameters)
            : base(parameters)
        {
        }

        public override MethodInfo GetDataReaderMethod()
            => GetStringMethod;

        public override System.Linq.Expressions.Expression CustomizeDataReaderExpression(System.Linq.Expressions.Expression expression)
            => System.Linq.Expressions.Expression.New(
                MemoryStreamConstructor,
                System.Linq.Expressions.Expression.Call(
                    System.Linq.Expressions.Expression.Property(null, UTF8Property),
                    EncodingGetBytesMethod,
                    expression));

        protected virtual string EscapeSqlLiteral(string literal)
            => literal.Replace("'", "''");

        protected override string GenerateNonNullSqlLiteral(object value)
            => $"'{EscapeSqlLiteral(JsonSerializer.Serialize(value))}'";

        protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
            => new BigQueryOwnedJsonTypeMapping(parameters);

        protected override void ConfigureParameter(DbParameter parameter)
        {
            if (parameter is Ivy.Data.BigQuery.BigQueryParameter bigQueryParameter)
            {
                bigQueryParameter.BigQueryDbType = Google.Cloud.BigQuery.V2.BigQueryDbType.String;
            }

            base.ConfigureParameter(parameter);
        }
    }
}