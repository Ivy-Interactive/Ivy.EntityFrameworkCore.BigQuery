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
    public class BigQueryTimeOnlyTypeMapping : TimeOnlyTypeMapping
    {
        private const string TimeFormatConst = "HH:mm:ss.ffffff";

        public BigQueryTimeOnlyTypeMapping(string storeType = "TIME")
            : base(
                new RelationalTypeMappingParameters(
                    new CoreTypeMappingParameters(typeof(TimeOnly), jsonValueReaderWriter: Microsoft.EntityFrameworkCore.Storage.Json.JsonTimeOnlyReaderWriter.Instance),
                    storeType,
                    StoreTypePostfix.None,
                    System.Data.DbType.Time))
        {
        }

        protected BigQueryTimeOnlyTypeMapping(RelationalTypeMappingParameters parameters)
            : base(parameters)
        {
        }

        protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
            => new BigQueryTimeOnlyTypeMapping(parameters);

        protected override string GenerateNonNullSqlLiteral(object value)
        {
            var t = (TimeOnly)value;

            return $"TIME '{t.ToString(TimeFormatConst, CultureInfo.InvariantCulture)}'";
        }

        protected override void ConfigureParameter(DbParameter parameter)
        {
            base.ConfigureParameter(parameter);

            if (parameter is BigQueryParameter bigQueryParameter)
            {
                bigQueryParameter.BigQueryDbType = Google.Cloud.BigQuery.V2.BigQueryDbType.Time;

                // Convert TimeOnly to TimeSpan for BigQuery
                if (parameter.Value is TimeOnly timeValue)
                {
                    parameter.Value = timeValue.ToTimeSpan();
                }
            }
        }
    }
}
