using Ivy.EntityFrameworkCore.BigQuery.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Ivy.EntityFrameworkCore.BigQuery.Extensions;

namespace Ivy.EntityFrameworkCore.BigQuery.Migrations
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

        [ConditionalFact(Skip = "BigQuery doesn't support table locking")]
        public override void Can_apply_one_migration_in_parallel() => base.Can_apply_one_migration_in_parallel();

        [ConditionalFact(Skip = "BigQuery doesn't support table locking")]
        public override Task Can_apply_one_migration_in_parallel_async() => Task.CompletedTask;

        [ConditionalFact(Skip = "BigQuery doesn't support table locking")]
        public override void Can_apply_second_migration_in_parallel() => base.Can_apply_second_migration_in_parallel();

        public override void Can_get_active_provider()
        {
            base.Can_get_active_provider();

            Assert.Equal("Ivy.EntityFrameworkCore.BigQuery", ActiveProvider);
        }

        protected override Task ExecuteSqlAsync(string value)
        => ((BigQueryTestStore)Fixture.TestStore).ExecuteNonQueryAsync(value);

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
