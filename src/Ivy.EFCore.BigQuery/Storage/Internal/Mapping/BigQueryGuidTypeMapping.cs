using Microsoft.EntityFrameworkCore.Storage;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ivy.EntityFrameworkCore.BigQuery.Storage.Internal.Mapping
{
    public class BigQueryGuidTypeMapping : GuidTypeMapping
    {
        public BigQueryGuidTypeMapping(string storeType = "STRING")
            : base(storeType, System.Data.DbType.String)
        {
        }

        protected BigQueryGuidTypeMapping(RelationalTypeMappingParameters parameters)
            : base(parameters)
        {
        }

        protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
            => new BigQueryGuidTypeMapping(parameters);


        protected override string GenerateNonNullSqlLiteral(object value)
        {
            var guidString = ((Guid)value).ToString();
            var escapedValue = guidString.Replace("'", "''");
            return $"'{escapedValue}'";
        }
    }
}
