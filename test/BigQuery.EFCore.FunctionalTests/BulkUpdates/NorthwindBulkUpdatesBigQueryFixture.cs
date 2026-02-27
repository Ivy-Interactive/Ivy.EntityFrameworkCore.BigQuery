using Ivy.Data.BigQuery;
using Ivy.EntityFrameworkCore.BigQuery.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.BulkUpdates;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.TestModels.Northwind;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace Ivy.EntityFrameworkCore.BigQuery.BulkUpdates;

public class NorthwindBulkUpdatesBigQueryFixture<TModelCustomizer> : NorthwindBulkUpdatesRelationalFixture<TModelCustomizer>
    where TModelCustomizer : ITestModelCustomizer, new()
{
    private bool _needsReseed;
    private string? _datasetName;

    protected override ITestStoreFactory TestStoreFactory
        => BigQueryNorthwindTestStoreFactory.Instance;

    protected override Type ContextType
        => typeof(TestModels.Northwind.NorthwindBigQueryContext);

    protected override void OnModelCreating(ModelBuilder modelBuilder, DbContext context)
    {
        base.OnModelCreating(modelBuilder, context);

        // BigQuery uses NUMERIC for money types
        modelBuilder.Entity<MostExpensiveProduct>()
            .Property(p => p.UnitPrice)
            .HasColumnType("NUMERIC");
    }

    public override void UseTransaction(DatabaseFacade facade, IDbContextTransaction transaction)
    {
        // BigQuery doesn't support transaction rollback, so we need to
        // reseed data before each test to ensure consistent test state.
        if (_needsReseed)
        {
            ReseedData(facade);
        }
        _needsReseed = true;
    }

    private void ReseedData(DatabaseFacade facade)
    {
        var connection = facade.GetDbConnection() as BigQueryConnection;
        if (connection == null) return;

        var builder = new BigQueryConnectionStringBuilder(connection.ConnectionString);
        _datasetName = builder.DefaultDatasetId;

        if (string.IsNullOrEmpty(_datasetName)) return;

        ReseedOrderDetails(connection);

        ResetCustomers(connection);
    }

    private void ReseedOrderDetails(BigQueryConnection connection)
    {
        var wasOpen = connection.State == System.Data.ConnectionState.Open;
        if (!wasOpen) connection.Open();

        try
        {
            long count;
            using (var countCmd = connection.CreateCommand())
            {
                countCmd.CommandTimeout = 300;
                countCmd.CommandText = $"SELECT COUNT(*) FROM `{_datasetName}`.`Order Details`";
                count = Convert.ToInt64(countCmd.ExecuteScalar());
            }

            if (count < 2155)
            {
                var reseedSql = GetOrderDetailsReseedSql();
                if (!string.IsNullOrEmpty(reseedSql))
                {
                    using var reseedCmd = connection.CreateCommand();
                    reseedCmd.CommandTimeout = 300;
                    reseedCmd.CommandText = reseedSql;
                    reseedCmd.ExecuteNonQuery();
                }
            }
        }
        finally
        {
            if (!wasOpen) connection.Close();
        }
    }

    private void ResetCustomers(BigQueryConnection connection)
    {
        var wasOpen = connection.State == System.Data.ConnectionState.Open;
        if (!wasOpen) connection.Open();

        try
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandTimeout = 300;

            // Reset any modified ContactName values back to original
            // This handles UPDATE tests that modify ContactName
            cmd.CommandText = $@"
                UPDATE `{_datasetName}`.`Customers` AS c
                SET `ContactName` = CASE `CustomerID`
                    WHEN 'ALFKI' THEN 'Maria Anders'
                    WHEN 'ANATR' THEN 'Ana Trujillo'
                    WHEN 'ANTON' THEN 'Antonio Moreno'
                    WHEN 'AROUT' THEN 'Thomas Hardy'
                    WHEN 'BERGS' THEN 'Christina Berglund'
                    WHEN 'BLAUS' THEN 'Hanna Moos'
                    WHEN 'BLONP' THEN 'Frédérique Citeaux'
                    WHEN 'BOLID' THEN 'Martín Sommer'
                    WHEN 'BONAP' THEN 'Laurence Lebihans'
                    WHEN 'BOTTM' THEN 'Elizabeth Lincoln'
                    WHEN 'BSBEV' THEN 'Victoria Ashworth'
                    WHEN 'CACTU' THEN 'Patricio Simpson'
                    WHEN 'CENTC' THEN 'Francisco Chang'
                    WHEN 'CHOPS' THEN 'Yang Wang'
                    WHEN 'COMMI' THEN 'Pedro Afonso'
                    WHEN 'CONSH' THEN 'Elizabeth Brown'
                    WHEN 'DRACD' THEN 'Sven Ottlieb'
                    WHEN 'DUMON' THEN 'Janine Labrune'
                    WHEN 'EASTC' THEN 'Ann Devon'
                    WHEN 'ERNSH' THEN 'Roland Mendel'
                    WHEN 'FAMIA' THEN 'Aria Cruz'
                    WHEN 'FISSA' THEN 'Diego Roel'
                    WHEN 'FOLIG' THEN 'Martine Rancé'
                    WHEN 'FOLKO' THEN 'Maria Larsson'
                    WHEN 'FRANK' THEN 'Peter Franken'
                    WHEN 'FRANR' THEN 'Carine Schmitt'
                    WHEN 'FRANS' THEN 'Paolo Accorti'
                    WHEN 'FURIB' THEN 'Lino Rodriguez'
                    WHEN 'GALED' THEN 'Eduardo Saavedra'
                    WHEN 'GODOS' THEN 'José Pedro Freyre'
                    WHEN 'GOURL' THEN 'André Fonseca'
                    WHEN 'GREAL' THEN 'Howard Snyder'
                    WHEN 'GROSR' THEN 'Manuel Pereira'
                    WHEN 'HANAR' THEN 'Mario Pontes'
                    WHEN 'HILAA' THEN 'Carlos Hernández'
                    WHEN 'HUNGC' THEN 'Yoshi Latimer'
                    WHEN 'HUNGO' THEN 'Patricia McKenna'
                    WHEN 'ISLAT' THEN 'Helen Bennett'
                    WHEN 'KOENE' THEN 'Philip Cramer'
                    WHEN 'LACOR' THEN 'Daniel Tonini'
                    WHEN 'LAMAI' THEN 'Annette Roulet'
                    WHEN 'LAUGB' THEN 'Yoshi Tannamuri'
                    WHEN 'LAZYK' THEN 'John Steel'
                    WHEN 'LEHMS' THEN 'Renate Messner'
                    WHEN 'LETSS' THEN 'Jaime Yorres'
                    WHEN 'LILAS' THEN 'Carlos González'
                    WHEN 'LINOD' THEN 'Felipe Izquierdo'
                    WHEN 'LONEP' THEN 'Fran Wilson'
                    WHEN 'MAGAA' THEN 'Giovanni Rovelli'
                    WHEN 'MAISD' THEN 'Catherine Dewey'
                    WHEN 'MEREP' THEN 'Jean Fresnière'
                    WHEN 'MORGK' THEN 'Alexander Feuer'
                    WHEN 'NORTS' THEN 'Simon Crowther'
                    WHEN 'OCEAN' THEN 'Yvonne Moncada'
                    WHEN 'OLDWO' THEN 'Rene Phillips'
                    WHEN 'OTTIK' THEN 'Henriette Pfalzheim'
                    WHEN 'PARIS' THEN 'Marie Bertrand'
                    WHEN 'PERIC' THEN 'Guillermo Fernández'
                    WHEN 'PICCO' THEN 'Georg Pipps'
                    WHEN 'PRINI' THEN 'Isabel de Castro'
                    WHEN 'QUEDE' THEN 'Bernardo Batista'
                    WHEN 'QUEEN' THEN 'Lúcia Carvalho'
                    WHEN 'QUICK' THEN 'Horst Kloss'
                    WHEN 'RANCH' THEN 'Sergio Gutiérrez'
                    WHEN 'RATTC' THEN 'Paula Wilson'
                    WHEN 'REGGC' THEN 'Maurizio Moroni'
                    WHEN 'RICAR' THEN 'Janete Limeira'
                    WHEN 'RICSU' THEN 'Michael Holz'
                    WHEN 'ROMEY' THEN 'Alejandra Camino'
                    WHEN 'SANTG' THEN 'Jonas Bergulfsen'
                    WHEN 'SAVEA' THEN 'Jose Pavarotti'
                    WHEN 'SEVES' THEN 'Hari Kumar'
                    WHEN 'SIMOB' THEN 'Jytte Petersen'
                    WHEN 'SPECD' THEN 'Dominique Perrier'
                    WHEN 'SPLIR' THEN 'Art Braunschweiger'
                    WHEN 'SUPRD' THEN 'Pascale Cartrain'
                    WHEN 'THEBI' THEN 'Liz Nixon'
                    WHEN 'THECR' THEN 'Liu Wong'
                    WHEN 'TOMSP' THEN 'Karin Josephs'
                    WHEN 'TORTU' THEN 'Miguel Angel Paolino'
                    WHEN 'TRADH' THEN 'Anabela Domingues'
                    WHEN 'TRAIH' THEN 'Helvetius Nagy'
                    WHEN 'VAFFE' THEN 'Palle Ibsen'
                    WHEN 'VICTE' THEN 'Mary Saveley'
                    WHEN 'VINET' THEN 'Paul Henriot'
                    WHEN 'WANDK' THEN 'Rita Müller'
                    WHEN 'WARTH' THEN 'Pirkko Koskitalo'
                    WHEN 'WELLI' THEN 'Paula Parente'
                    WHEN 'WHITC' THEN 'Karl Jablonski'
                    WHEN 'WILMK' THEN 'Matti Karttunen'
                    WHEN 'WOLZA' THEN 'Zbyszek Piestrzeniewicz'
                    ELSE `ContactName`
                END
                WHERE `ContactName` = 'Updated' OR `ContactName` != CASE `CustomerID`
                    WHEN 'ALFKI' THEN 'Maria Anders'
                    ELSE `ContactName`
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

    private string GetOrderDetailsReseedSql()
    {
        var scriptPath = Path.Combine(
            Path.GetDirectoryName(typeof(NorthwindBulkUpdatesBigQueryFixture<>).Assembly.Location)!,
            "Northwind.sql");

        if (!File.Exists(scriptPath))
            return string.Empty;

        var script = File.ReadAllText(scriptPath);

        var startMarker = "INSERT INTO `efc_northwind`.`Order Details`";
        var startIndex = script.IndexOf(startMarker);
        if (startIndex < 0) return string.Empty;

        var endIndex = script.IndexOf("\nGO", startIndex);
        if (endIndex < 0) endIndex = script.Length;

        var insertSql = script.Substring(startIndex, endIndex - startIndex);

        insertSql = insertSql.Replace("`efc_northwind`", $"`{_datasetName}`");

        return $@"
DELETE FROM `{_datasetName}`.`Order Details` WHERE TRUE;

{insertSql}";
    }
}
