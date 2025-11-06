using Ivy.EntityFrameworkCore.BigQuery.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ivy.EntityFrameworkCore.BigQuery.Infrastructure
{
    public class BigQueryDbContextOptionsBuilder : RelationalDbContextOptionsBuilder<BigQueryDbContextOptionsBuilder, BigQueryOptionsExtension>
    {
        public BigQueryDbContextOptionsBuilder(DbContextOptionsBuilder optionsBuilder) : base(optionsBuilder)
        {
        }
    }
}
