using Ivy.EntityFrameworkCore.BigQuery.Design.Internal;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Scaffolding;
using Microsoft.EntityFrameworkCore.Scaffolding.Metadata;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Ivy.EntityFrameworkCore.BigQuery.Extensions;

namespace Ivy.EntityFrameworkCore.BigQuery.TestUtilities;

public class BigQueryDatabaseCleaner : RelationalDatabaseCleaner
{
    protected override IDatabaseModelFactory CreateDatabaseModelFactory(ILoggerFactory loggerFactory)
    {
        var services = new ServiceCollection();
        services.AddEntityFrameworkBigQuery();

        new BigQueryDesignTimeServices().ConfigureDesignTimeServices(services);

        return services
            .BuildServiceProvider()
            .GetRequiredService<IDatabaseModelFactory>();
    }

    public override void Clean(DatabaseFacade facade)
    {
        var creator = facade.GetService<IRelationalDatabaseCreator>();
        var connection = facade.GetService<IRelationalConnection>();
        var loggerFactory = facade.GetService<ILoggerFactory>();

        if (!creator.Exists())
        {
            creator.Create();
            facade.EnsureCreated();
            return;
        }

        //BQdoesn't support TRUNCATE
        var databaseModelFactory = CreateDatabaseModelFactory(loggerFactory);
        var databaseModel = databaseModelFactory.Create(
            connection.DbConnection,
            new DatabaseModelFactoryOptions());

        connection.Open();
        try
        {
            foreach (var table in databaseModel.Tables.Where(AcceptTable))
            {
                var tableName = table.Schema != null
                    ? $"`{table.Schema}`.`{table.Name}`"
                    : $"`{table.Name}`";

                using var command = connection.DbConnection.CreateCommand();
                command.CommandText = $"DELETE FROM {tableName} WHERE true";
                command.ExecuteNonQuery();
            }
        }
        finally
        {
            connection.Close();
        }

        facade.EnsureCreated();
    }
}