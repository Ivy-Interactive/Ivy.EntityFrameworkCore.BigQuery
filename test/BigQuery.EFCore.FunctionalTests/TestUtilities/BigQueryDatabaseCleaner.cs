using Ivy.EntityFrameworkCore.BigQuery.Design.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Scaffolding;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Ivy.EntityFrameworkCore.BigQuery.Extensions;

namespace Ivy.EntityFrameworkCore.BigQuery.FunctionalTests.TestUtilities;

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
       
    }
}