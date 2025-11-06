using Ivy.Data.BigQuery;
using Microsoft.EntityFrameworkCore.Storage;
using System.Data.Common;
using System.Globalization;

namespace Ivy.EntityFrameworkCore.BigQuery.Storage.Internal.Mapping
{
    public class BigQueryByteTypeMapping : ByteTypeMapping
    {
        private static readonly Type _clrType = typeof(byte);

        public BigQueryByteTypeMapping()
            : this("INT64")
        {
        }

        protected BigQueryByteTypeMapping(string storeType)
             : this(
                new RelationalTypeMappingParameters(
                    new CoreTypeMappingParameters(_clrType),
                    storeType,
                    StoreTypePostfix.None,
                    System.Data.DbType.Byte
                ))
        {
        }

        protected BigQueryByteTypeMapping(RelationalTypeMappingParameters parameters)
            : base(parameters)
        {
        }

        protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
            => new BigQueryByteTypeMapping(parameters);

        protected override string GenerateNonNullSqlLiteral(object value)
            => ((byte)value).ToString(CultureInfo.InvariantCulture);

        protected override void ConfigureParameter(DbParameter parameter)
        {
            base.ConfigureParameter(parameter);

            if (parameter is BigQueryParameter bigQueryParameter)
            {
                bigQueryParameter.BigQueryDbType = Google.Cloud.BigQuery.V2.BigQueryDbType.Int64;
            }
        }
    }
}