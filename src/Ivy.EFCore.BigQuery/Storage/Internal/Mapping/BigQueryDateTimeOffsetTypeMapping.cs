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
    public class BigQueryDateTimeOffsetTypeMapping : DateTimeOffsetTypeMapping
    {
        private const string TimestampFormatConst = "yyyy-MM-dd HH:mm:ss.ffffff zzz";

        public BigQueryDateTimeOffsetTypeMapping(string storeType = "TIMESTAMP")
            : base(
                new RelationalTypeMappingParameters(
                    new CoreTypeMappingParameters(typeof(DateTimeOffset), jsonValueReaderWriter: Microsoft.EntityFrameworkCore.Storage.Json.JsonDateTimeOffsetReaderWriter.Instance),
                    storeType,
                    StoreTypePostfix.None,
                    System.Data.DbType.DateTimeOffset))
        {
        }

        protected BigQueryDateTimeOffsetTypeMapping(RelationalTypeMappingParameters parameters)
            : base(parameters)
        {
        }

        protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
            => new BigQueryDateTimeOffsetTypeMapping(parameters);

        protected override string GenerateNonNullSqlLiteral(object value)
        {
            var dto = (DateTimeOffset)value;

            return $"TIMESTAMP '{dto.ToString(TimestampFormatConst, CultureInfo.InvariantCulture)}'";
        }
    }
}
