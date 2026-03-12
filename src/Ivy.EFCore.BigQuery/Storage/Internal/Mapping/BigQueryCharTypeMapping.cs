using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.Json;

namespace Ivy.EntityFrameworkCore.BigQuery.Storage.Internal.Mapping;

/// <summary>
/// Maps char to BigQuery STRING type.
/// </summary>
public class BigQueryCharTypeMapping : RelationalTypeMapping
{
    public BigQueryCharTypeMapping()
        : base("STRING",
            typeof(char),
            System.Data.DbType.StringFixedLength,
            jsonValueReaderWriter: JsonCharReaderWriter.Instance)
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