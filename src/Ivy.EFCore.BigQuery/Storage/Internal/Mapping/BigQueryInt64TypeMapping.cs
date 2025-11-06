using Ivy.Data.BigQuery;
using Microsoft.EntityFrameworkCore.Storage;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ivy.EntityFrameworkCore.BigQuery.Storage.Internal.Mapping
{
    public class BigQueryInt64TypeMapping : LongTypeMapping
    {
        private static readonly Type _clrType = typeof(long);
        public BigQueryInt64TypeMapping()
            : this("INT64")
        {
        }

        protected BigQueryInt64TypeMapping(string storeType)
             : this(
                new RelationalTypeMappingParameters(
                    new CoreTypeMappingParameters(_clrType),
                    storeType,
                    StoreTypePostfix.None,
                    System.Data.DbType.Int64
                ))
        {
        }

        protected BigQueryInt64TypeMapping(RelationalTypeMappingParameters parameters)
            : base(parameters)
        {
        }

        protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
            => new BigQueryInt64TypeMapping(parameters);

        protected override string GenerateNonNullSqlLiteral(object value)
            => ((long)value).ToString(CultureInfo.InvariantCulture);

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
