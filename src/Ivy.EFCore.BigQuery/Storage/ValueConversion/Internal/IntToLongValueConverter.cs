using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ivy.EntityFrameworkCore.BigQuery.Storage.ValueConversion.Internal
{
    public class IntToLongValueConverter : ValueConverter<int, long>
    {
        public IntToLongValueConverter(ConverterMappingHints? mappingHints = null)
            : base(
 
                  v => (long)v,
                  v => Convert.ToInt32(v),

                  mappingHints
                  )
        {
        }
    }
}
