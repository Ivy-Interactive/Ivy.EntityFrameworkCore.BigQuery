using Microsoft.EntityFrameworkCore.Storage;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace Ivy.EntityFrameworkCore.BigQuery.Storage.Internal.Mapping
{
    public class BigQueryStringTypeMapping : StringTypeMapping
    {
        
        private static readonly Type _clrType = typeof(string);

        public BigQueryStringTypeMapping()
            : this("STRING")
        {
        }

        protected BigQueryStringTypeMapping(string storeType)
             : this(
                new RelationalTypeMappingParameters(
                    new CoreTypeMappingParameters(_clrType),
                    storeType,
                    StoreTypePostfix.None,
                    System.Data.DbType.String,
                    unicode: true, //Always Unicode (UTF-8).
                    size: null,
                    fixedLength: false
                ))
        {
        }

        protected BigQueryStringTypeMapping(RelationalTypeMappingParameters parameters)
            : base(parameters)
        {
        }

        protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
            => new BigQueryStringTypeMapping(parameters);

        protected override string GenerateNonNullSqlLiteral(object value)
        {
            var stringValue = (string)value;
            var escapedValue = stringValue.Replace("\\", "\\\\").Replace("'", "\\'");
            return $"'{escapedValue}'";
        }
    }
}
