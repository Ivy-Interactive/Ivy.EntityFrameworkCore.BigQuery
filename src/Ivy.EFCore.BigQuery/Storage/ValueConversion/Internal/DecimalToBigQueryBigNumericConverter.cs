using Google.Cloud.BigQuery.V2;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using System.Globalization;

namespace Ivy.EntityFrameworkCore.BigQuery.Storage.ValueConversion.Internal;

/// <summary>
/// Converts decimal values to BigQueryBigNumeric for BIGNUMERIC columns.
/// </summary>
public class DecimalToBigQueryBigNumericConverter : ValueConverter<decimal, BigQueryBigNumeric>
{
    public DecimalToBigQueryBigNumericConverter(ConverterMappingHints? mappingHints = null)
        : base(
              v => BigQueryBigNumeric.Parse(v.ToString(CultureInfo.InvariantCulture)),
              v => v.ToDecimal(LossOfPrecisionHandling.Truncate),
              mappingHints
              )
    {
    }
}