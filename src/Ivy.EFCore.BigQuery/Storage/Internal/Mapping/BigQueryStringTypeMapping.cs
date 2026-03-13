using Ivy.Data.BigQuery;
using Microsoft.EntityFrameworkCore.Storage;
using System.Data.Common;

namespace Ivy.EntityFrameworkCore.BigQuery.Storage.Internal.Mapping
{
    public class BigQueryStringTypeMapping : StringTypeMapping
    {
        private static readonly Type _clrType = typeof(string);

        public BigQueryStringTypeMapping()
            : this("STRING")
        {
        }

        protected BigQueryStringTypeMapping(string storeType)
             : this(
                new RelationalTypeMappingParameters(
                    new CoreTypeMappingParameters(_clrType, jsonValueReaderWriter: Microsoft.EntityFrameworkCore.Storage.Json.JsonStringReaderWriter.Instance),
                    storeType,
                    StoreTypePostfix.None,
                    System.Data.DbType.String,
                    unicode: true, //Always Unicode (UTF-8).
                    size: null,
                    fixedLength: false
                ))
        {
        }

        protected BigQueryStringTypeMapping(RelationalTypeMappingParameters parameters)
            : base(parameters)
        {
        }

        protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
            => new BigQueryStringTypeMapping(parameters);

        public override string GenerateSqlLiteral(object? value)
        {
            if (value == null)
            {
                return "NULL";
            }
            
            var stringValue = value as string ?? value.ToString()!;
            return GenerateNonNullSqlLiteral(stringValue);
        }

        protected override string GenerateNonNullSqlLiteral(object value)
        {
            var stringValue = value as string ?? value.ToString()!;
            var escapedValue = stringValue
                .Replace("\\", "\\\\")
                .Replace("'", "\\'")
                .Replace("\n", "\\n")
                .Replace("\r", "\\r")
                .Replace("\t", "\\t");
            return $"'{escapedValue}'";
        }

        protected override void ConfigureParameter(DbParameter parameter)
        {
            base.ConfigureParameter(parameter);

            if (parameter is BigQueryParameter bigQueryParameter)
            {
                bigQueryParameter.BigQueryDbType = Google.Cloud.BigQuery.V2.BigQueryDbType.String;
            }
        }
    }
}
