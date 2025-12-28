using Ivy.Data.BigQuery;
using Microsoft.EntityFrameworkCore.Storage;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ivy.EntityFrameworkCore.BigQuery.Storage.Internal.Mapping
{
    public class BigQueryDateOnlyTypeMapping : DateOnlyTypeMapping
    {
        private const string DateFormatConst = "yyyy-MM-dd";

        public BigQueryDateOnlyTypeMapping(string storeType = "DATE")
            : base(storeType,System.Data.DbType.Date)
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
