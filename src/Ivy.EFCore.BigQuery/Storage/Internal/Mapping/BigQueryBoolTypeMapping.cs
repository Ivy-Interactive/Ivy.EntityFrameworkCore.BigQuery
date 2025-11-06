using Microsoft.EntityFrameworkCore.Storage;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ivy.EntityFrameworkCore.BigQuery.Storage.Internal.Mapping
{
    public class BigQueryBoolTypeMapping : BoolTypeMapping
    {
        public BigQueryBoolTypeMapping(string storeType = "BOOL")
            : base(storeType, System.Data.DbType.Boolean)
        {
        }

        protected BigQueryBoolTypeMapping(RelationalTypeMappingParameters parameters)
            : base(parameters)
        {
        }

        protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
            => new BigQueryBoolTypeMapping(parameters);
        protected override string GenerateNonNullSqlLiteral(object value)
            => (bool)value ? "TRUE" : "FALSE";
    }
}
