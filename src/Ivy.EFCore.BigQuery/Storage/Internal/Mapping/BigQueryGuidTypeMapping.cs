using Microsoft.EntityFrameworkCore.Storage;

namespace Ivy.EntityFrameworkCore.BigQuery.Storage.Internal.Mapping
{
    public class BigQueryGuidTypeMapping : GuidTypeMapping
    {
        public BigQueryGuidTypeMapping(string storeType = "STRING")
            : base(
                new RelationalTypeMappingParameters(
                    new CoreTypeMappingParameters(typeof(Guid), jsonValueReaderWriter: Microsoft.EntityFrameworkCore.Storage.Json.JsonGuidReaderWriter.Instance),
                    storeType,
                    StoreTypePostfix.None,
                    System.Data.DbType.String))
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
