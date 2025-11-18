using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace Ivy.EntityFrameworkCore.BigQuery.TestUtilities
{
    public class BigQueryNorthwindModelCustomizer : ITestModelCustomizer
    {
        public void Customize(ModelBuilder modelBuilder, DbContext context)
        {
        }

        public void ConfigureConventions(ModelConfigurationBuilder configurationBuilder)
        {
        }
    }
}
