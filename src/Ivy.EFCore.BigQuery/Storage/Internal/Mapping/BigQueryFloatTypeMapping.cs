using Ivy.Data.BigQuery;
using Microsoft.EntityFrameworkCore.Storage;
using System.Data.Common;
using System.Globalization;

namespace Ivy.EntityFrameworkCore.BigQuery.Storage.Internal.Mapping
{
    public class BigQueryFloatTypeMapping : FloatTypeMapping
    {
        private static readonly Type _clrType = typeof(float);

        public BigQueryFloatTypeMapping()
            : this("FLOAT64")
        {
        }

        protected BigQueryFloatTypeMapping(string storeType)
             : this(
                new RelationalTypeMappingParameters(
                    new CoreTypeMappingParameters(_clrType),
                    storeType,
                    StoreTypePostfix.None,
                    System.Data.DbType.Single
                ))
        {
        }

        protected BigQueryFloatTypeMapping(RelationalTypeMappingParameters parameters)
            : base(parameters)
        {
        }

        protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
            => new BigQueryFloatTypeMapping(parameters);

        protected override string GenerateNonNullSqlLiteral(object value)
        {
            var floatValue = (float)value;

            if (float.IsNaN(floatValue))
                return "CAST('NaN' AS FLOAT64)";
            if (float.IsPositiveInfinity(floatValue))
                return "CAST('inf' AS FLOAT64)";
            if (float.IsNegativeInfinity(floatValue))
                return "CAST('-inf' AS FLOAT64)";

            return floatValue.ToString("R", CultureInfo.InvariantCulture);
        }

        protected override void ConfigureParameter(DbParameter parameter)
        {
            base.ConfigureParameter(parameter);

            if (parameter is BigQueryParameter bigQueryParameter)
            {
                bigQueryParameter.BigQueryDbType = Google.Cloud.BigQuery.V2.BigQueryDbType.Float64;
            }
        }
    }
}