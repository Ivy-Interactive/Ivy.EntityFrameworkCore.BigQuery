using Ivy.Data.BigQuery;
using Ivy.EntityFrameworkCore.BigQuery.Storage.ValueConversion.Internal;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using System.Data.Common;
using System.Globalization;

namespace Ivy.EntityFrameworkCore.BigQuery.Storage.Internal.Mapping;

public class BigQueryDecimalTypeMapping : RelationalTypeMapping
{
    public BigQueryDecimalTypeMapping(string storeType = "BIGNUMERIC(57, 28)")
        : base(
            new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(
                    typeof(decimal),
                    GetConverterForStoreType(storeType),
                    jsonValueReaderWriter: Microsoft.EntityFrameworkCore.Storage.Json.JsonDecimalReaderWriter.Instance
                ),
                storeType,
                StoreTypePostfix.PrecisionAndScale,
                System.Data.DbType.Object
            ))
    {
    }

    protected BigQueryDecimalTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    private static ValueConverter GetConverterForStoreType(string storeType)
    {        
        return storeType.StartsWith("BIG", StringComparison.OrdinalIgnoreCase)
            ? new DecimalToBigQueryBigNumericConverter()
            : new DecimalToBigQueryNumericConverter();
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
    {        
        var isBigNumeric = parameters.StoreType.StartsWith("BIG", StringComparison.OrdinalIgnoreCase);
        var currentIsBigNumeric = Parameters.StoreType.StartsWith("BIG", StringComparison.OrdinalIgnoreCase);

        if (isBigNumeric != currentIsBigNumeric)
        {
            return new BigQueryDecimalTypeMapping(parameters.StoreType);
        }

        return new BigQueryDecimalTypeMapping(parameters);
    }

    protected override string GenerateNonNullSqlLiteral(object value)
    {
        string stringValue;
        if (value is decimal decimalValue)
        {
            stringValue = decimalValue.ToString(CultureInfo.InvariantCulture);
        }
        else if (value is Google.Cloud.BigQuery.V2.BigQueryBigNumeric bigQueryBigNumeric)
        {
            stringValue = bigQueryBigNumeric.ToString();
        }
        else if (value is Google.Cloud.BigQuery.V2.BigQueryNumeric bigQueryNumeric)
        {
            stringValue = bigQueryNumeric.ToString();
        }
        else
        {
            stringValue = value.ToString() ?? "0";
        }

        string typePrefix = Parameters.StoreType.StartsWith("BIG", StringComparison.OrdinalIgnoreCase)
            ? "BIGNUMERIC"
            : "NUMERIC";

        return $"{typePrefix} '{stringValue}'";
    }

    protected override void ConfigureParameter(DbParameter parameter)
    {
        base.ConfigureParameter(parameter);

        if (parameter is BigQueryParameter bigQueryParameter)
        {
            bigQueryParameter.BigQueryDbType = Parameters.StoreType.StartsWith("BIG", StringComparison.OrdinalIgnoreCase)
                ? Google.Cloud.BigQuery.V2.BigQueryDbType.BigNumeric
                : Google.Cloud.BigQuery.V2.BigQueryDbType.Numeric;
        }
    }
}
