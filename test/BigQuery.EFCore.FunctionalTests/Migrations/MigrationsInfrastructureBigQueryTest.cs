using Ivy.EntityFrameworkCore.BigQuery.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static Microsoft.EntityFrameworkCore.Migrations.MigrationsInfrastructureFixtureBase;
using Ivy.EntityFrameworkCore.BigQuery.Extensions;

namespace Ivy.EntityFrameworkCore.BigQuery.FunctionalTests.Migrations
{
    public class MigrationsInfrastructureBigQueryTest(MigrationsInfrastructureBigQueryTest.MigrationsInfrastructureBigQueryFixture fixture) : MigrationsInfrastructureTestBase<MigrationsInfrastructureBigQueryTest.MigrationsInfrastructureBigQueryFixture>(fixture)
    {
        public override void Can_diff_against_2_1_ASP_NET_Identity_model()
        {
            throw new NotImplementedException();
        }

        public override void Can_diff_against_2_2_ASP_NET_Identity_model()
        {
            throw new NotImplementedException();
        }

        public override void Can_diff_against_2_2_model()
        {
            throw new NotImplementedException();
        }

        public override void Can_diff_against_3_0_ASP_NET_Identity_model()
        {
            throw new NotImplementedException();
        }

        public override void Can_get_active_provider()
        {
            base.Can_get_active_provider();

            Assert.Equal("Ivy.EFCore.BigQuery", ActiveProvider);
        }

        protected override Task ExecuteSqlAsync(string value)
        {
            throw new NotImplementedException();
        }

        public class MigrationsInfrastructureBigQueryFixture : MigrationsInfrastructureFixtureBase
        {
            protected override ITestStoreFactory TestStoreFactory
                => BigQueryTestStoreFactory.Instance;

            public override MigrationsContext CreateContext()
            {
                var options = AddOptions(
                        TestStore.AddProviderOptions(new DbContextOptionsBuilder())
                            .UseBigQuery(TestStore.ConnectionString))
                    .UseInternalServiceProvider(ServiceProvider)
                    .Options;
                return new MigrationsContext(options);
            }
        }
    }
}
