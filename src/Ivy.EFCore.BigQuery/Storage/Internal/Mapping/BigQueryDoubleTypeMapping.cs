using Ivy.Data.BigQuery;
using Microsoft.EntityFrameworkCore.Storage;
using System.Data.Common;
using System.Globalization;

namespace Ivy.EntityFrameworkCore.BigQuery.Storage.Internal.Mapping
{
    public class BigQueryDoubleTypeMapping : DoubleTypeMapping
    {
        public BigQueryDoubleTypeMapping(string storeType = "FLOAT64")
            : base(storeType, System.Data.DbType.Double)
        {
        }

        protected BigQueryDoubleTypeMapping(RelationalTypeMappingParameters parameters)
            : base(parameters)
        {
        }

        protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
            => new BigQueryDoubleTypeMapping(parameters);


        protected override string GenerateNonNullSqlLiteral(object value)
        {
            var doubleValue = Convert.ToDouble(value);

            if (double.IsNaN(doubleValue))
                return "CAST('NaN' AS FLOAT64)";
            if (double.IsPositiveInfinity(doubleValue))
                return "CAST('inf' AS FLOAT64)";
            if (double.IsNegativeInfinity(doubleValue))
                return "CAST('-inf' AS FLOAT64)";

            return doubleValue.ToString("G17", CultureInfo.InvariantCulture);
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
