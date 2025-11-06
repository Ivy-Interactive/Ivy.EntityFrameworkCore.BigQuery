using System.Diagnostics;
using System.Linq;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.TestModels.Northwind;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace Ivy.EntityFrameworkCore.BigQuery.FunctionalTests.TestUtilities
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
