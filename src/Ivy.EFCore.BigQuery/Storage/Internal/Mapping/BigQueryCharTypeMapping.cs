using Microsoft.EntityFrameworkCore.Storage;

namespace Ivy.EntityFrameworkCore.BigQuery.Storage.Internal.Mapping;

/// <summary>
/// Maps char to BigQuery STRING type.
/// </summary>
public class BigQueryCharTypeMapping : RelationalTypeMapping
{
    public BigQueryCharTypeMapping()
        : base(new RelationalTypeMappingParameters(
            new CoreTypeMappingParameters(typeof(char)),
            "STRING",
            StoreTypePostfix.None,
            System.Data.DbType.StringFixedLength,
            unicode: false,
            size: 1,
            fixedLength: true))
    {
    }

    protected BigQueryCharTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new BigQueryCharTypeMapping(parameters);

    protected override string GenerateNonNullSqlLiteral(object value)
    {
        var charValue = (char)value;
        // Escape single quotes by doubling them
        return charValue == '\'' ? "''''" : $"'{charValue}'";
    }
}