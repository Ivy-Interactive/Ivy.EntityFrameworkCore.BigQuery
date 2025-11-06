using Microsoft.EntityFrameworkCore.Storage;
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ivy.EntityFrameworkCore.BigQuery.Storage.Internal.Mapping
{
    public class BigQueryDateTimeTypeMapping : DateTimeTypeMapping
    {
        private const string DateTimeFormatConst = "yyyy-MM-dd HH:mm:ss.ffffff";

        public BigQueryDateTimeTypeMapping(string storeType = "DATETIME")
            : base(storeType, System.Data.DbType.DateTime)
        {
        }

        protected BigQueryDateTimeTypeMapping(RelationalTypeMappingParameters parameters)
            : base(parameters)
        {
        }

        protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
            => new BigQueryDateTimeTypeMapping(parameters);


        protected override string GenerateNonNullSqlLiteral(object value)
        {
            var dt = (DateTime)value;
            return $"DATETIME '{dt.ToString(DateTimeFormatConst, CultureInfo.InvariantCulture)}'";
        }
    }
}
