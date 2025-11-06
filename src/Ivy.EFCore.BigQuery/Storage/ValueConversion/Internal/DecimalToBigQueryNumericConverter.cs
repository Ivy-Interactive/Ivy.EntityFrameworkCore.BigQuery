using Google.Cloud.BigQuery.V2;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ivy.EntityFrameworkCore.BigQuery.Storage.ValueConversion.Internal
{
    public class DecimalToBigQueryNumericConverter : ValueConverter<decimal, BigQueryBigNumeric>
    {
        public DecimalToBigQueryNumericConverter(ConverterMappingHints? mappingHints = null)
            : base(
                  v => BigQueryBigNumeric.Parse(v.ToString(CultureInfo.InvariantCulture)),
                  v => v.ToDecimal(LossOfPrecisionHandling.Truncate),
                  mappingHints
                  )
        {
        }
    }
}
