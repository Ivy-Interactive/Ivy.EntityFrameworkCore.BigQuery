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
    public class BigQueryTimeOnlyTypeMapping : TimeOnlyTypeMapping
    {
        private const string TimeFormatConst = "HH:mm:ss.ffffff";

        public BigQueryTimeOnlyTypeMapping(string storeType = "TIME")
            : base(storeType, System.Data.DbType.Time)
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
    }
}
