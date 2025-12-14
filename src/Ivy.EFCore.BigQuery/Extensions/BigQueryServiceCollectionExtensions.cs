using Ivy.EntityFrameworkCore.BigQuery.Diagnostics;
using Ivy.EntityFrameworkCore.BigQuery.Infrastructure;
using Ivy.EntityFrameworkCore.BigQuery.Infrastructure.Internal;
using Ivy.EntityFrameworkCore.BigQuery.Metadata.Conventions;
using Ivy.EntityFrameworkCore.BigQuery.Migrations;
using Ivy.EntityFrameworkCore.BigQuery.Query.Internal;
using Ivy.EntityFrameworkCore.BigQuery.Storage.Internal;
using Ivy.EntityFrameworkCore.BigQuery.Update.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;
using Microsoft.Extensions.DependencyInjection;

namespace Ivy.EntityFrameworkCore.BigQuery.Extensions
{
    public static class BigQueryServiceCollectionExtensions
    {

        public static IServiceCollection AddBigQuery<TContext>(
       this IServiceCollection serviceCollection,
       string connectionString,
       string databaseName,
       Action<BigQueryDbContextOptionsBuilder>? bigQueryOptionsAction = null,
       Action<DbContextOptionsBuilder>? optionsAction = null)
       where TContext : DbContext
       => serviceCollection.AddDbContext<TContext>(
           (serviceProvider, options) =>
           {
               optionsAction?.Invoke(options);
               options.UseBigQuery(connectionString, bigQueryOptionsAction);
           });

        public static IServiceCollection AddEntityFrameworkBigQuery(this IServiceCollection serviceCollection)
        {
            var builder = new EntityFrameworkRelationalServicesBuilder(serviceCollection)
              .TryAdd<LoggingDefinitions, BigQueryLoggingDefinitions>()
              .TryAdd<IDatabaseProvider, DatabaseProvider<BigQueryOptionsExtension>>()
              .TryAdd<IRelationalTypeMappingSource, BigQueryTypeMappingSource>()
              .TryAdd<ISqlGenerationHelper, BigQuerySqlGenerationHelper>()
              .TryAdd<IRelationalAnnotationProvider, RelationalAnnotationProvider>()
              .TryAdd<IProviderConventionSetBuilder, BigQueryConventionSetBuilder>()
              .TryAdd<IModificationCommandBatchFactory, BigQueryModificationCommandBatchFactory>()
              .TryAdd<IRelationalDatabaseCreator, BigQueryDatabaseCreator>()
              .TryAdd<IHistoryRepository, BigQueryHistoryRepository>()
              .TryAdd<IRelationalConnection>(p => p.GetRequiredService<IBigQueryRelationalConnection>())
              .TryAdd<IMigrationsSqlGenerator>(p =>
                  new BigQueryMigrationsSqlGenerator(
                      p.GetRequiredService<MigrationsSqlGeneratorDependencies>(),
                      p.GetService<IDbContextOptions>()))
              .TryAdd<IMemberTranslatorProvider, BigQueryMemberTranslatorProvider>()
              .TryAdd<IUpdateSqlGenerator, BigQueryUpdateSqlGenerator>()
              .TryAdd<ISqlExpressionFactory, BigQuerySqlExpressionFactory>()
              .TryAdd<IMethodCallTranslatorProvider, BigQueryMethodCallTranslatorProvider>()
              .TryAdd<IAggregateMethodCallTranslatorProvider, BigQueryAggregateMethodCallTranslatorProvider>()
              .TryAdd<IQuerySqlGeneratorFactory, BigQueryQuerySqlGeneratorFactory>()
              .TryAdd<IExecutionStrategyFactory, BigQueryExecutionStrategyFactory>()
              .TryAdd<IQueryableMethodTranslatingExpressionVisitorFactory, BigQueryQueryableMethodTranslatingExpressionVisitorFactory>()
              .TryAdd<IRelationalSqlTranslatingExpressionVisitorFactory, BigQuerySqlTranslatingExpressionVisitorFactory>()
              .TryAdd<IRelationalParameterBasedSqlProcessorFactory, BigQueryRelationalParameterBasedSqlProcessorFactory>()
              .TryAdd<IQueryCompilationContextFactory, BigQueryQueryCompilationContextFactory>()
              
              .TryAddProviderSpecificServices(
                  s =>
                  {
                      s.TryAddScoped<IBigQueryRelationalConnection, BigQueryRelationalConnection>();
                      s.TryAddScoped<IBigQueryUpdateSqlGenerator>(p => (IBigQueryUpdateSqlGenerator)p.GetRequiredService<IUpdateSqlGenerator>());
                  })
              .TryAddCoreServices();
            return serviceCollection;
        }
    }
}