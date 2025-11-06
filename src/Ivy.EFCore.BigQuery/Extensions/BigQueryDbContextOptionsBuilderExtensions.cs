using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using System.Buffers.Text;
using Ivy.EntityFrameworkCore.BigQuery.Infrastructure;
using Ivy.EntityFrameworkCore.BigQuery.Infrastructure.Internal;

namespace Ivy.EntityFrameworkCore.BigQuery.Extensions
{
    public static class BigQueryDbContextOptionsBuilderExtensions
    {
        public static DbContextOptionsBuilder UseBigQuery(
            this DbContextOptionsBuilder optionsBuilder,
            string connectionString,
            Action<BigQueryDbContextOptionsBuilder>? bigQueryOptionsAction = null)
        {        
            var extension = optionsBuilder.Options.FindExtension<BigQueryOptionsExtension>()
                            ?? new BigQueryOptionsExtension();

            extension = (BigQueryOptionsExtension)extension.WithConnectionString(connectionString);

            ((IDbContextOptionsBuilderInfrastructure)optionsBuilder).AddOrUpdateExtension(extension);

            bigQueryOptionsAction?.Invoke(new BigQueryDbContextOptionsBuilder(optionsBuilder));

            return optionsBuilder;
        }

        //public static DbContextOptionsBuilder UseBigQuery(
        //    this DbContextOptionsBuilder optionsBuilder,
        //    Action<BigQueryDbContextOptionsBuilder>? bigQueryOptionsAction = null)
        //{
        //    // Check.NotNull(optionsBuilder, nameof(optionsBuilder));
        //    var extension = optionsBuilder.Options.FindExtension<BigQueryOptionsExtension>()
        //                    ?? new BigQueryOptionsExtension();
        //    ((IDbContextOptionsBuilderInfrastructure)optionsBuilder).AddOrUpdateExtension(extension);
        //    bigQueryOptionsAction?.Invoke(new BigQueryDbContextOptionsBuilder(optionsBuilder));
        //    return optionsBuilder;

        //}
    }
}
