using Ivy.Data.BigQuery;
using Microsoft.EntityFrameworkCore.Storage;
using System.Data.Common;
using System.Globalization;

namespace Ivy.EntityFrameworkCore.BigQuery.Storage.Internal.Mapping
{
    public class BigQueryIntTypeMapping : IntTypeMapping
    {
        private static readonly Type _clrType = typeof(int);

        public BigQueryIntTypeMapping()
            : this("INT64")
        {
        }

        protected BigQueryIntTypeMapping(string storeType)
             : this(
                new RelationalTypeMappingParameters(
                    new CoreTypeMappingParameters(_clrType, jsonValueReaderWriter: Microsoft.EntityFrameworkCore.Storage.Json.JsonInt32ReaderWriter.Instance),
                    storeType,
                    StoreTypePostfix.None,
                    System.Data.DbType.Int32
                ))
        {
        }

        protected BigQueryIntTypeMapping(RelationalTypeMappingParameters parameters)
            : base(parameters)
        {
        }

        protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
            => new BigQueryIntTypeMapping(parameters);

        protected override string GenerateNonNullSqlLiteral(object value)
            => Convert.ToInt32(value).ToString(CultureInfo.InvariantCulture);

        protected override void ConfigureParameter(DbParameter parameter)
        {
            base.ConfigureParameter(parameter);

            if (parameter is BigQueryParameter bigQueryParameter)
            {
                bigQueryParameter.BigQueryDbType = Google.Cloud.BigQuery.V2.BigQueryDbType.Int64;

                // Convert int to long for BigQuery
                if (parameter.Value is int intValue)
                {
                    parameter.Value = (long)intValue;
                }
            }
        }
    }
}