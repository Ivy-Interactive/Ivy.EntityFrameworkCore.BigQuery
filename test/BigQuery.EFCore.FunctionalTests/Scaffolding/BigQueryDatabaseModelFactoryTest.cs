using Ivy.EntityFrameworkCore.BigQuery.Design.Internal;
using Ivy.EntityFrameworkCore.BigQuery.Diagnostics;
using Ivy.EntityFrameworkCore.BigQuery.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Diagnostics.Internal;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Scaffolding;
using Microsoft.EntityFrameworkCore.Scaffolding.Metadata;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System.Diagnostics;

namespace Ivy.EntityFrameworkCore.BigQuery.Scaffolding
{
    public class BigQueryDatabaseModelFactoryTest : IClassFixture<BigQueryDatabaseModelFactoryTest.BigQueryDatabaseModelFixture>
    {
        protected BigQueryDatabaseModelFixture Fixture { get; set; }

        public BigQueryDatabaseModelFactoryTest(BigQueryDatabaseModelFixture fixture)
        {
            Fixture = fixture;
            Fixture.ListLoggerFactory.Clear();
        }

        private void Test(
            string createSql,
            IEnumerable<string> tables,
            IEnumerable<string> schemas,
            Action<DatabaseModel> asserter,
            string cleanupSql)
        {
            Fixture.TestStore.ExecuteNonQuery(createSql);

            try
            {
                var services = new ServiceCollection()
                    .AddSingleton<TypeMappingSourceDependencies>()
                    .AddSingleton<RelationalTypeMappingSourceDependencies>()
                    .AddSingleton<ValueConverterSelectorDependencies>()
                    .AddSingleton<DiagnosticSource>(new DiagnosticListener(DbLoggerCategory.Name))
                    .AddSingleton<ILoggingOptions, LoggingOptions>()
                    .AddSingleton<LoggingDefinitions, BigQueryLoggingDefinitions>()
                    .AddSingleton(typeof(IDiagnosticsLogger<>), typeof(DiagnosticsLogger<>))
                    .AddSingleton<IValueConverterSelector, ValueConverterSelector>()
                    .AddSingleton<ILoggerFactory>(Fixture.ListLoggerFactory)
                    .AddSingleton<IDbContextLogger, NullDbContextLogger>();

                new BigQueryDesignTimeServices().ConfigureDesignTimeServices(services);

                var databaseModelFactory = services
                    .BuildServiceProvider()
                    .GetRequiredService<IDatabaseModelFactory>();

                var databaseModel = databaseModelFactory.Create(
                    Fixture.TestStore.ConnectionString,
                    new DatabaseModelFactoryOptions(tables, schemas));
                Assert.NotNull(databaseModel);
                asserter(databaseModel);
            }
            finally
            {
                if (!string.IsNullOrEmpty(cleanupSql))
                {
                    Fixture.TestStore.ExecuteNonQuery(cleanupSql);
                }
            }
        }

        #region Table

        [ConditionalFact]
        public void Create_tables()
    => Test(
        @"
CREATE TABLE Everest ( id INT64 );
CREATE TABLE Denali ( id INT64 );",
        Enumerable.Empty<string>(),
        Enumerable.Empty<string>(),
        dbModel =>
        {
            Assert.Collection(
                dbModel.Tables.OrderBy(t => t.Name),
                d => Assert.Equal("Denali", d.Name),
                e => Assert.Equal("Everest", e.Name));
        },
        @"
DROP TABLE Everest;
DROP TABLE Denali;");

        [ConditionalFact]
        public void Create_columns()
            => Test(
                @"
CREATE TABLE MountainsColumns (
    Id INT64 NOT NULL,
    Name STRING NOT NULL
);",
                Enumerable.Empty<string>(),
                Enumerable.Empty<string>(),
                dbModel =>
                {
                    var table = dbModel.Tables.Single();

                    Assert.Equal(2, table.Columns.Count);
                    Assert.All(
                        table.Columns, c => Assert.Equal("MountainsColumns", c.Table.Name));

                    Assert.Single(table.Columns.Where(c => c.Name == "Id"));
                    Assert.Single(table.Columns.Where(c => c.Name == "Name"));
                },
                "DROP TABLE MountainsColumns;");


        [Fact]
        public void Create_primary_key()
    => Test(
        """
CREATE TABLE PrimaryKeyTable (
    Id INT64 NOT NULL,
    PRIMARY KEY (Id) NOT ENFORCED
);
""",
        [],
        [],
        dbModel =>
        {
            var pk = dbModel.Tables.Single().PrimaryKey;

            Assert.Equal("BigQueryDatabaseModelFactoryTest", pk.Table.Schema);
            Assert.Equal("PrimaryKeyTable", pk.Table.Name);
            Assert.StartsWith("PK_PrimaryKeyTable", pk.Name);
            Assert.Equal(["Id"], pk.Columns.Select(ic => ic.Name).ToList());
        },
        """
DROP TABLE PrimaryKeyTable;
""");

        [ConditionalFact]
        public void Create_foreign_keys()
    => Test(
        @"
CREATE TABLE PrincipalTable (
    Id INT64,
    PRIMARY KEY (Id) NOT ENFORCED
);

CREATE TABLE FirstDependent (
    Id INT64,
    ForeignKeyId INT64,
    PRIMARY KEY (Id) NOT ENFORCED,
    FOREIGN KEY (ForeignKeyId) REFERENCES PrincipalTable(Id) NOT ENFORCED
);

CREATE TABLE SecondDependent (
    Id INT64,
    PRIMARY KEY (Id) NOT ENFORCED,
    FOREIGN KEY (Id) REFERENCES PrincipalTable(Id) NOT ENFORCED
);",
        Enumerable.Empty<string>(),
        Enumerable.Empty<string>(),
        dbModel =>
        {
            var firstFk = Assert.Single(dbModel.Tables.Single(t => t.Name == "FirstDependent").ForeignKeys);

            Assert.Equal("FirstDependent", firstFk.Table.Name);
            Assert.Equal("PrincipalTable", firstFk.PrincipalTable.Name);
            Assert.Equal(["ForeignKeyId"], firstFk.Columns.Select(ic => ic.Name).ToList());
            Assert.Equal(["Id"], firstFk.PrincipalColumns.Select(ic => ic.Name).ToList());
            Assert.Equal(ReferentialAction.NoAction, firstFk.OnDelete);

            var secondFk = Assert.Single(dbModel.Tables.Single(t => t.Name == "SecondDependent").ForeignKeys);

            Assert.Equal("SecondDependent", secondFk.Table.Name);
            Assert.Equal("PrincipalTable", secondFk.PrincipalTable.Name);
            Assert.Equal(["Id"], secondFk.Columns.Select(ic => ic.Name).ToList());
            Assert.Equal(["Id"], secondFk.PrincipalColumns.Select(ic => ic.Name).ToList());
            Assert.Equal(ReferentialAction.NoAction, secondFk.OnDelete);
        },
        @"
DROP TABLE SecondDependent;
DROP TABLE FirstDependent;
DROP TABLE PrincipalTable;");
        #endregion


        public class BigQueryDatabaseModelFixture : SharedStoreFixtureBase<PoolableDbContext>
        {
            protected override string StoreName
            => nameof(BigQueryDatabaseModelFactoryTest);

            protected override ITestStoreFactory TestStoreFactory
                => BigQueryTestStoreFactory.Instance;

            public new BigQueryTestStore TestStore
                => (BigQueryTestStore)base.TestStore;

            protected override bool ShouldLogCategory(string logCategory)
                => logCategory == DbLoggerCategory.Scaffolding.Name;
        }
    }
}
