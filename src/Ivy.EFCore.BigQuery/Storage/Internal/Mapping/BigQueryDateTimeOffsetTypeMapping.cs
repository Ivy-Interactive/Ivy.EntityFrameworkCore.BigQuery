using Microsoft.EntityFrameworkCore.Storage;
using System.Globalization;

namespace Ivy.EntityFrameworkCore.BigQuery.Storage.Internal.Mapping
{
    public class BigQueryDateTimeOffsetTypeMapping : DateTimeOffsetTypeMapping
    {
        private const string TimestampFormatConst = "yyyy-MM-dd HH:mm:ss.ffffffzzz";

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
            var dto = value switch
            {
                DateTimeOffset dateTimeOffset => dateTimeOffset,
                DateTime dateTime => new DateTimeOffset(dateTime),
                _ => throw new InvalidCastException($"Cannot convert {value.GetType()} to DateTimeOffset")
            };

            return $"TIMESTAMP '{dto.ToString(TimestampFormatConst, CultureInfo.InvariantCulture)}'";
        }
    }
}
