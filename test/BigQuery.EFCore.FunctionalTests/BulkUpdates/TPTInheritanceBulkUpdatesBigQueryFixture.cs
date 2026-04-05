using Ivy.Data.BigQuery;
using Ivy.EntityFrameworkCore.BigQuery.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.BulkUpdates;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.TestModels.InheritanceModel;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace Ivy.EntityFrameworkCore.BigQuery.BulkUpdates;

public class TPTInheritanceBulkUpdatesBigQueryFixture : TPTInheritanceBulkUpdatesFixture
{
    // BQ doesn't support auto-generated keys
    public override bool UseGeneratedKeys => false;

    // Disable pooling to avoid service provider issues with relational facade dependencies
    protected override bool UsePooling => false;

    protected override ITestStoreFactory TestStoreFactory
        => BigQueryTestStoreFactory.Instance;

    private bool _needsReseed;

    // BQ doesn't support transactions- reseed data between tests
    public override void UseTransaction(DatabaseFacade facade, IDbContextTransaction transaction)
    {
        if (_needsReseed)
        {
            ReseedAnimals(facade);
        }
        _needsReseed = true;
    }

    private void ReseedAnimals(DatabaseFacade facade)
    {
        var connection = facade.GetDbConnection() as BigQueryConnection;
        if (connection == null) return;

        var builder = new BigQueryConnectionStringBuilder(connection.ConnectionString);
        var dataset = builder.DefaultDatasetId;
        if (string.IsNullOrEmpty(dataset)) return;

        var wasOpen = connection.State == System.Data.ConnectionState.Open;
        if (!wasOpen) connection.Open();

        try
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandTimeout = 300;
            // Reset
            cmd.CommandText = $@"
                UPDATE `{dataset}`.`Animals` AS a
                SET `Name` = CASE a.`Id`
                    WHEN 1 THEN 'Great spotted kiwi'
                    WHEN 2 THEN 'Bald eagle'
                    ELSE a.`Name`
                END
                WHERE a.`Name` != CASE a.`Id`
                    WHEN 1 THEN 'Great spotted kiwi'
                    WHEN 2 THEN 'Bald eagle'
                    ELSE a.`Name`
                END";
            cmd.ExecuteNonQuery();
        }
        catch
        {
        }
        finally
        {
            if (!wasOpen) connection.Close();
        }
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder, DbContext context)
    {
        base.OnModelCreating(modelBuilder, context);

        modelBuilder.Entity<Animal>().Property(e => e.Id).ValueGeneratedNever();
        modelBuilder.Entity<Drink>().Property(e => e.Id).ValueGeneratedNever();
    }
}
