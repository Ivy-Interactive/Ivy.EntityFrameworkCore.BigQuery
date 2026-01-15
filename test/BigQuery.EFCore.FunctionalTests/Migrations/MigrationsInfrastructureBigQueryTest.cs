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

        [ConditionalFact(Skip = "BigQuery doesn't support DDL in transactions")]
        public override Task Can_generate_idempotent_up_and_down_scripts() => base.Can_generate_idempotent_up_and_down_scripts();

        [ConditionalFact(Skip = "BigQuery doesn't support DDL in transactions")]
        public override Task Can_generate_up_and_down_scripts() => base.Can_generate_up_and_down_scripts();

        [ConditionalFact(Skip = "BigQuery doesn't support DDL in transactions")]
        public override Task Can_generate_up_and_down_script_using_names() => base.Can_generate_up_and_down_script_using_names();

        [ConditionalFact(Skip = "BigQuery doesn't support DDL in transactions")]
        public override Task Can_generate_one_up_and_down_script() => base.Can_generate_one_up_and_down_script();

        [ConditionalFact(Skip = "BigQuery doesn't support table locking")]
        public override void Can_apply_one_migration_in_parallel() => base.Can_apply_one_migration_in_parallel();

        [ConditionalFact(Skip = "BigQuery doesn't support table locking")]
        public override Task Can_apply_one_migration_in_parallel_async() => base.Can_apply_one_migration_in_parallel_async();

        [ConditionalFact(Skip = "BigQuery doesn't support table locking")]
        public override void Can_apply_second_migration_in_parallel() => base.Can_apply_second_migration_in_parallel();

        [ConditionalFact(Skip = "BigQuery doesn't support table locking")]
        public override Task Can_apply_second_migration_in_parallel_async() => base.Can_apply_second_migration_in_parallel_async();

        [ConditionalFact(Skip = "BigQuery doesn't support DDL in transactions")]
        public override void Can_apply_two_migrations_in_transaction() => base.Can_apply_two_migrations_in_transaction();

        [ConditionalFact(Skip = "BigQuery doesn't support DDL in transactions")]
        public override Task Can_apply_two_migrations_in_transaction_async() => base.Can_apply_two_migrations_in_transaction_async();

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
