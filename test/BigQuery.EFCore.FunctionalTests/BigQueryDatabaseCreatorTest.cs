using Ivy.EntityFrameworkCore.BigQuery.TestUtilities;
using Ivy.EntityFrameworkCore.BigQuery.Storage.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.Extensions.DependencyInjection;
using Ivy.EntityFrameworkCore.BigQuery.Extensions;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace Ivy.EntityFrameworkCore.BigQuery;

public class BigQueryDatabaseCreatorTest
{

}

public abstract class SqlServerDatabaseCreatorTestBase
{
    protected static IDisposable CreateTransactionScope(bool useTransaction)
        => TestStore.CreateTransactionScope(useTransaction);

    protected static TestDatabaseCreator GetDatabaseCreator(BigQueryTestStore testStore)
        => GetDatabaseCreator(testStore.ConnectionString);

    protected static TestDatabaseCreator GetDatabaseCreator(string connectionString)
        => GetDatabaseCreator(new BloggingContext(connectionString));

    protected static TestDatabaseCreator GetDatabaseCreator(BloggingContext context)
        => (TestDatabaseCreator)context.GetService<IRelationalDatabaseCreator>();

    protected static IExecutionStrategy GetExecutionStrategy(BigQueryTestStore testStore)
        => new BloggingContext(testStore).GetService<IExecutionStrategyFactory>().Create();

    // ReSharper disable once ClassNeverInstantiated.Local
    private class TestSqlServerExecutionStrategyFactory(ExecutionStrategyDependencies dependencies)
        : BigQueryExecutionStrategyFactory(dependencies)
    {
        protected override IExecutionStrategy CreateDefaultStrategy(ExecutionStrategyDependencies dependencies)
            => new NonRetryingExecutionStrategy(dependencies);
    }

    private static IServiceProvider CreateServiceProvider()
        => new ServiceCollection()
            .AddEntityFrameworkBigQuery()
            .AddScoped<IExecutionStrategyFactory, TestSqlServerExecutionStrategyFactory>()
            .AddScoped<IRelationalDatabaseCreator, TestDatabaseCreator>()
            .BuildServiceProvider(validateScopes: true);

    protected class BloggingContext(
        string connectionString,
        bool seed = false,
        bool asyncSeed = false)
        : DbContext
    {
        private readonly string _connectionString = connectionString;

        public BloggingContext(BigQueryTestStore testStore)
            : this(testStore.ConnectionString)
        {
        }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            optionsBuilder
                .UseBigQuery(_connectionString, b => b.ApplyConfiguration())
                .UseInternalServiceProvider(CreateServiceProvider());
            if (seed)
            {
                optionsBuilder.UseSeeding((_, __) => { });
            }

            if (asyncSeed)
            {
                optionsBuilder.UseAsyncSeeding((_, __, ___) => Task.CompletedTask);
            }
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => modelBuilder.Entity<Blog>(
                b =>
                {
                    b.HasKey(
                        e => new { e.Key1, e.Key2 });
                    b.Property(e => e.AndRow).IsConcurrencyToken().ValueGeneratedOnAddOrUpdate();
                });

        public DbSet<Blog> Blogs { get; set; }
    }

    public class Blog
    {
        public string Key1 { get; set; }
        public byte[] Key2 { get; set; }
        public string Cheese { get; set; }
        public int ErMilan { get; set; }
        public bool George { get; set; }
        public Guid TheGu { get; set; }
        public DateTime NotFigTime { get; set; }
        public byte ToEat { get; set; }
        public double OrNothing { get; set; }
        public short Fuse { get; set; }
        public long WayRound { get; set; }
        public float On { get; set; }
        public byte[] AndChew { get; set; }
        public byte[] AndRow { get; set; }
    }

    public class TestDatabaseCreator(
        RelationalDatabaseCreatorDependencies dependencies,
        IBigQueryRelationalConnection connection,
        IRawSqlCommandBuilder rawSqlCommandBuilder) : BigQueryDatabaseCreator(dependencies, connection, rawSqlCommandBuilder)
    {
        public bool HasTablesBase()
            => HasTables();

        public Task<bool> HasTablesAsyncBase(CancellationToken cancellationToken = default)
            => HasTablesAsync(cancellationToken);

        public IExecutionStrategy ExecutionStrategy
            => Dependencies.ExecutionStrategy;
    }
}
