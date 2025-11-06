using Google.Cloud.BigQuery.V2;
using Microsoft.EntityFrameworkCore.Storage;
using System.Data.Common;

namespace Ivy.EntityFrameworkCore.BigQuery.Storage.Internal.Mapping
{
    public class BigQueryBigNumericTypeMapping : RelationalTypeMapping
    {
        private static readonly Type _clrType = typeof(BigQueryBigNumeric);

        public BigQueryBigNumericTypeMapping(string storeType)
            : this(
                new RelationalTypeMappingParameters(
                    new CoreTypeMappingParameters(_clrType),
                    storeType,
                    StoreTypePostfix.PrecisionAndScale,
                    System.Data.DbType.Object
                ))
        {
        }

        protected BigQueryBigNumericTypeMapping(RelationalTypeMappingParameters parameters)
            : base(parameters)
        {
        }

        protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
            => new BigQueryBigNumericTypeMapping(parameters);

        protected override string GenerateNonNullSqlLiteral(object value)
        {
            var numericValue = (BigQueryBigNumeric)value;
            string typePrefix = Parameters.StoreType.StartsWith("BIG", StringComparison.OrdinalIgnoreCase)
                ? "BIGNUMERIC" : "NUMERIC";
            return $"{typePrefix} '{numericValue}'";
        }


        protected override void ConfigureParameter(DbParameter parameter)
        {
            base.ConfigureParameter(parameter);

            if (parameter is Data.BigQuery.BigQueryParameter bigQueryParameter)
            {
                bigQueryParameter.BigQueryDbType = BigQueryDbType.BigNumeric;
            }

            if (Parameters.Precision.HasValue)
            {
                parameter.Precision = (byte)Parameters.Precision.Value;
            }
            if (Parameters.Scale.HasValue)
            {
                parameter.Scale = (byte)Parameters.Scale.Value;
            }
        }
    }
}
