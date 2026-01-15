using Ivy.Data.BigQuery;
using Microsoft.EntityFrameworkCore.Storage;
using System.Data.Common;
using System.Globalization;

namespace Ivy.EntityFrameworkCore.BigQuery.Storage.Internal.Mapping
{
    public class BigQueryShortTypeMapping : ShortTypeMapping
    {
        private static readonly Type _clrType = typeof(short);

        public BigQueryShortTypeMapping()
            : this("INT64")
        {
        }

        protected BigQueryShortTypeMapping(string storeType)
             : this(
                new RelationalTypeMappingParameters(
                    new CoreTypeMappingParameters(_clrType, jsonValueReaderWriter: Microsoft.EntityFrameworkCore.Storage.Json.JsonInt16ReaderWriter.Instance),
                    storeType,
                    StoreTypePostfix.None,
                    System.Data.DbType.Int16
                ))
        {
        }

        protected BigQueryShortTypeMapping(RelationalTypeMappingParameters parameters)
            : base(parameters)
        {
        }

        protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
            => new BigQueryShortTypeMapping(parameters);

        protected override string GenerateNonNullSqlLiteral(object value)
            => Convert.ToInt16(value).ToString(CultureInfo.InvariantCulture);

        protected override void ConfigureParameter(DbParameter parameter)
        {
            base.ConfigureParameter(parameter);

            if (parameter is BigQueryParameter bigQueryParameter)
            {
                bigQueryParameter.BigQueryDbType = Google.Cloud.BigQuery.V2.BigQueryDbType.Int64;

                // Convert short to long for BigQuery
                if (parameter.Value is short shortValue)
                {
                    parameter.Value = (long)shortValue;
                }
            }
        }
    }
}