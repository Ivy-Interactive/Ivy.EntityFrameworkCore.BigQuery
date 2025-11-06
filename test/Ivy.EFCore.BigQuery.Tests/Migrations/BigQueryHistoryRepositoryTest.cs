using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Xunit;
using Ivy.EntityFrameworkCore.BigQuery.FunctionalTests.TestUtilities;
using Ivy.EntityFrameworkCore.BigQuery.Extensions;

namespace Ivy.EntityFrameworkCore.BigQuery.Migrations;

public class BigQueryHistoryRepositoryTest
{
    [ConditionalFact]
    public void GetCreateScript_works()
    {
        var sql = CreateHistoryRepository().GetCreateScript();

        Assert.Equal(
            """
CREATE TABLE `__EFMigrationsHistory` (
    `MigrationId` STRING NOT NULL,
    `ProductVersion` STRING NOT NULL,
    PRIMARY KEY (`MigrationId`) NOT ENFORCED 
);

""", sql, ignoreLineEndingDifferences: true);
    }

    [ConditionalFact]
   public void GetCreateScript_works_with_schema()
    {
        throw new NotImplementedException();
        var sql = CreateHistoryRepository("my").GetCreateScript();

        //Assert.Equal("", sql, ignoreLineEndingDifferences: true);
    }

    [ConditionalFact]
    public void GetCreateIfNotExistsScript_works()
    {
        var sql = CreateHistoryRepository().GetCreateIfNotExistsScript();

        Assert.Equal(
            """
CREATE TABLE IF NOT EXISTS `__EFMigrationsHistory` (
    `MigrationId` STRING NOT NULL,
    `ProductVersion` STRING NOT NULL,
    PRIMARY KEY (`MigrationId`) NOT ENFORCED 
);

""", sql, ignoreLineEndingDifferences: true);
    }

    [ConditionalFact]
    public void GetCreateIfNotExistsScript_works_with_schema()
    {
        throw new NotImplementedException();
        var sql = CreateHistoryRepository("my").GetCreateIfNotExistsScript();

        Assert.Equal("", sql, ignoreLineEndingDifferences: true);
    }

    private static IHistoryRepository CreateHistoryRepository(string schema = null)
        => new TestDbContext(
                new DbContextOptionsBuilder()
                    .UseInternalServiceProvider(BigQueryTestHelpers.Instance.CreateServiceProvider())
                    .UseBigQuery(
                    "AuthMethod=ApplicationDefaultCredentials;ProjectId=dummyproject;DefaultDatasetId=dummydataset",
                    b => b.MigrationsHistoryTable(HistoryRepository.DefaultTableName, schema))
            .Options)
            .GetService<IHistoryRepository>();

    [ConditionalFact]
    public void GetDeleteScript_works()
    {
        var sql = CreateHistoryRepository().GetDeleteScript("Migration1");

        Assert.Equal(
            """
DELETE FROM `__EFMigrationsHistory`
WHERE `MigrationId` = 'Migration1';

""", sql, ignoreLineEndingDifferences: true);
    }

    [ConditionalFact]
    public void GetInsertScript_works()
    {
        var sql = CreateHistoryRepository().GetInsertScript(
            new HistoryRow("Migration1", "7.0.0"));

        Assert.Equal(
            """
INSERT INTO `__EFMigrationsHistory` (`MigrationId`, `ProductVersion`)
VALUES ('Migration1', '7.0.0');

""", sql, ignoreLineEndingDifferences: true);
    }

    [ConditionalFact]
    public void GetBeginIfNotExistsScript_works()
    {
        var sql = CreateHistoryRepository().GetBeginIfNotExistsScript("Migration1");

        Assert.Equal(
            """
BEGIN
IF NOT EXISTS(SELECT 1 FROM `__EFMigrationsHistory` WHERE `MigrationId` = 'Migration1') THEN
""", sql, ignoreLineEndingDifferences: true);
    }

    [ConditionalFact]
    public void GetBeginIfExistsScript_works()
    {
        var sql = CreateHistoryRepository().GetBeginIfExistsScript("Migration1");

        Assert.Equal(
            """
BEGIN
IF EXISTS(SELECT 1 FROM `__EFMigrationsHistory` WHERE `MigrationId` = 'Migration1') THEN
""", sql, ignoreLineEndingDifferences: true);
    }

    [ConditionalFact]
    public void GetEndIfScript_works()
    {
        var sql = CreateHistoryRepository().GetEndIfScript();

        Assert.Equal(
            """
END IF;
END;
""", sql, ignoreLineEndingDifferences: true);
    }

    private class TestDbContext(DbContextOptions options) : DbContext(options)
    {
        public DbSet<Blog> Blogs { get; set; }

        [DbFunction("TableFunction")]
        public IQueryable<TableFunction> TableFunction()
            => FromExpression(() => TableFunction());

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
        }
    }

    private class Blog
    {
        public int Id { get; set; }
    }

    private class TableFunction
    {
        public int Id { get; set; }
        public int BlogId { get; set; }
        public Blog Blog { get; set; }
    }
}