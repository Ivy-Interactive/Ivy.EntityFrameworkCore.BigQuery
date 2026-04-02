using Ivy.Data.BigQuery;
using Microsoft.EntityFrameworkCore.Storage;
using System.Data.Common;
using System.Globalization;

namespace Ivy.EntityFrameworkCore.BigQuery.Storage.Internal.Mapping
{
    public class BigQueryDateOnlyTypeMapping : DateOnlyTypeMapping
    {
        private const string DateFormatConst = "yyyy-MM-dd";

        public BigQueryDateOnlyTypeMapping(string storeType = "DATE")
            : base(
                new RelationalTypeMappingParameters(
                    new CoreTypeMappingParameters(typeof(DateOnly), jsonValueReaderWriter: Microsoft.EntityFrameworkCore.Storage.Json.JsonDateOnlyReaderWriter.Instance),
                    storeType,
                    StoreTypePostfix.None,
                    System.Data.DbType.Date))
        {
        }

        protected BigQueryDateOnlyTypeMapping(RelationalTypeMappingParameters parameters)
            : base(parameters)
        {
        }

        protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
            => new BigQueryDateOnlyTypeMapping(parameters);


        protected override string GenerateNonNullSqlLiteral(object value)
        {
            var d = (DateOnly)value;
            return $"DATE '{d.ToString(DateFormatConst, CultureInfo.InvariantCulture)}'";
        }

        protected override void ConfigureParameter(DbParameter parameter)
        {
            base.ConfigureParameter(parameter);

            if (parameter is BigQueryParameter bigQueryParameter)
            {
                bigQueryParameter.BigQueryDbType = Google.Cloud.BigQuery.V2.BigQueryDbType.Date;

                // Convert DateOnly to DateTime for BigQuery
                if (parameter.Value is DateOnly dateValue)
                {
                    parameter.Value = dateValue.ToDateTime(TimeOnly.MinValue);
                }
            }
        }
    }
}
