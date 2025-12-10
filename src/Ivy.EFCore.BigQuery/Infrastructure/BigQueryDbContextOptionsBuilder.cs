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

        /// <summary>
        /// Configures whether to silently ignore UNIQUE constraints instead of throwing an exception.
        /// When enabled, UNIQUE constraints are skipped during table creation with a warning logged.
        /// Default is false (throws NotSupportedException).
        /// </summary>
        /// <param name="ignoreUniqueConstraints">True to ignore UNIQUE constraints with a warning, false to throw an exception.</param>
        /// <returns>The same builder instance so that multiple calls can be chained.</returns>
        public virtual BigQueryDbContextOptionsBuilder IgnoreUniqueConstraints(bool ignoreUniqueConstraints = true)
        {
            var extension = OptionsBuilder.Options.FindExtension<BigQueryOptionsExtension>() ?? new BigQueryOptionsExtension();
            extension = extension.WithIgnoreUniqueConstraints(ignoreUniqueConstraints);

            ((IDbContextOptionsBuilderInfrastructure)OptionsBuilder).AddOrUpdateExtension(extension);

            return this;
        }
    }
}
