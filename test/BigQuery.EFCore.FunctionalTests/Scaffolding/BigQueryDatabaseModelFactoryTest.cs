using Ivy.EntityFrameworkCore.BigQuery.Design.Internal;
using Ivy.EntityFrameworkCore.BigQuery.Diagnostics;
using Ivy.EntityFrameworkCore.BigQuery.NetTopologySuite.Design.Internal;
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
using NetTopologySuite.Geometries;
using System.Diagnostics;

#pragma warning disable EF1001 // Internal EF Core API usage.

namespace Ivy.EntityFrameworkCore.BigQuery.Scaffolding;

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

    #region Array

    [ConditionalFact]
    public void Create_array_columns_simple_types()
        => Test(
            @"
CREATE TABLE ArrayTable (
    Id INT64 NOT NULL,
    IntArray ARRAY<INT64>,
    StringArray ARRAY<STRING>,
    FloatArray ARRAY<FLOAT64>,
    BoolArray ARRAY<BOOL>
);",
            Enumerable.Empty<string>(),
            Enumerable.Empty<string>(),
            dbModel =>
            {
                var table = dbModel.Tables.Single();
                Assert.Equal("ArrayTable", table.Name);
                Assert.Equal(5, table.Columns.Count);

                var idCol = table.Columns.Single(c => c.Name == "Id");
                Assert.Equal("INT64", idCol.StoreType);
                Assert.False(idCol.IsNullable);

                var intArrayCol = table.Columns.Single(c => c.Name == "IntArray");
                Assert.Equal("ARRAY<INT64>", intArrayCol.StoreType);
                Assert.True(intArrayCol.IsNullable); // BigQuery arrays are always nullable

                var stringArrayCol = table.Columns.Single(c => c.Name == "StringArray");
                Assert.Equal("ARRAY<STRING>", stringArrayCol.StoreType);
                Assert.True(stringArrayCol.IsNullable);

                var floatArrayCol = table.Columns.Single(c => c.Name == "FloatArray");
                Assert.Equal("ARRAY<FLOAT64>", floatArrayCol.StoreType);
                Assert.True(floatArrayCol.IsNullable);

                var boolArrayCol = table.Columns.Single(c => c.Name == "BoolArray");
                Assert.Equal("ARRAY<BOOL>", boolArrayCol.StoreType);
                Assert.True(boolArrayCol.IsNullable);
            },
            "DROP TABLE ArrayTable;");

    [ConditionalFact]
    public void Create_array_columns_date_time_types()
        => Test(
            @"
CREATE TABLE DateTimeArrayTable (
    Id INT64 NOT NULL,
    DateArray ARRAY<DATE>,
    TimeArray ARRAY<TIME>,
    DateTimeArray ARRAY<DATETIME>,
    TimestampArray ARRAY<TIMESTAMP>
);",
            Enumerable.Empty<string>(),
            Enumerable.Empty<string>(),
            dbModel =>
            {
                var table = dbModel.Tables.Single();
                Assert.Equal("DateTimeArrayTable", table.Name);
                Assert.Equal(5, table.Columns.Count);

                var dateArrayCol = table.Columns.Single(c => c.Name == "DateArray");
                Assert.Equal("ARRAY<DATE>", dateArrayCol.StoreType);
                Assert.True(dateArrayCol.IsNullable);

                var timeArrayCol = table.Columns.Single(c => c.Name == "TimeArray");
                Assert.Equal("ARRAY<TIME>", timeArrayCol.StoreType);
                Assert.True(timeArrayCol.IsNullable);

                var dateTimeArrayCol = table.Columns.Single(c => c.Name == "DateTimeArray");
                Assert.Equal("ARRAY<DATETIME>", dateTimeArrayCol.StoreType);
                Assert.True(dateTimeArrayCol.IsNullable);

                var timestampArrayCol = table.Columns.Single(c => c.Name == "TimestampArray");
                Assert.Equal("ARRAY<TIMESTAMP>", timestampArrayCol.StoreType);
                Assert.True(timestampArrayCol.IsNullable);
            },
            "DROP TABLE DateTimeArrayTable;");

    [ConditionalFact]
    public void Create_array_columns_numeric_types()
        => Test(
            @"
CREATE TABLE NumericArrayTable (
    Id INT64 NOT NULL,
    NumericArray ARRAY<NUMERIC>,
    BigNumericArray ARRAY<BIGNUMERIC>,
    BytesArray ARRAY<BYTES>
);",
            Enumerable.Empty<string>(),
            Enumerable.Empty<string>(),
            dbModel =>
            {
                var table = dbModel.Tables.Single();
                Assert.Equal("NumericArrayTable", table.Name);
                Assert.Equal(4, table.Columns.Count);

                var numericArrayCol = table.Columns.Single(c => c.Name == "NumericArray");
                Assert.Equal("ARRAY<NUMERIC>", numericArrayCol.StoreType);
                Assert.True(numericArrayCol.IsNullable);

                var bigNumericArrayCol = table.Columns.Single(c => c.Name == "BigNumericArray");
                Assert.Equal("ARRAY<BIGNUMERIC>", bigNumericArrayCol.StoreType);
                Assert.True(bigNumericArrayCol.IsNullable);

                var bytesArrayCol = table.Columns.Single(c => c.Name == "BytesArray");
                Assert.Equal("ARRAY<BYTES>", bytesArrayCol.StoreType);
                Assert.True(bytesArrayCol.IsNullable);
            },
            "DROP TABLE NumericArrayTable;");

    [ConditionalFact]
    public void Create_array_of_struct_columns()
        => Test(
            @"
CREATE TABLE ArrayOfStructTable (
    Id INT64 NOT NULL,
    ContactInfos ARRAY<STRUCT<email STRING, phone STRING, city STRING>>
);",
            Enumerable.Empty<string>(),
            Enumerable.Empty<string>(),
            dbModel =>
            {
                // Should have 2 tables: the main table and the generated struct type table
                Assert.Equal(2, dbModel.Tables.Count);

                var mainTable = dbModel.Tables.Single(t => t.Name == "ArrayOfStructTable");
                Assert.Equal(2, mainTable.Columns.Count);

                var idCol = mainTable.Columns.Single(c => c.Name == "Id");
                Assert.Equal("INT64", idCol.StoreType);
                Assert.False(idCol.IsNullable);

                var contactInfosCol = mainTable.Columns.Single(c => c.Name == "ContactInfos");
                Assert.Equal("ARRAY<STRUCT<email STRING, phone STRING, city STRING>>", contactInfosCol.StoreType);
                Assert.True(contactInfosCol.IsNullable); // Arrays are always nullable in BigQuery

                // Verify the struct type table was generated (singularized from ContactInfos -> ContactInfo)
                var structTable = dbModel.Tables.SingleOrDefault(t => t.Name == "ContactInfo");
                Assert.NotNull(structTable);
                Assert.True((bool)structTable["BigQuery:IsStructType"]!);
                Assert.Equal(3, structTable.Columns.Count);

                var emailCol = structTable.Columns.Single(c => c.Name == "email");
                Assert.Equal("STRING", emailCol.StoreType);

                var phoneCol = structTable.Columns.Single(c => c.Name == "phone");
                Assert.Equal("STRING", phoneCol.StoreType);

                var cityCol = structTable.Columns.Single(c => c.Name == "city");
                Assert.Equal("STRING", cityCol.StoreType);
            },
            "DROP TABLE ArrayOfStructTable;");

    #endregion

    #region JSON

    [ConditionalFact]
    public void Create_json_columns()
        => Test(
            @"
CREATE TABLE JsonTable (
    Id INT64 NOT NULL,
    Metadata JSON,
    Settings JSON NOT NULL
);",
            Enumerable.Empty<string>(),
            Enumerable.Empty<string>(),
            dbModel =>
            {
                var table = dbModel.Tables.Single();
                Assert.Equal("JsonTable", table.Name);
                Assert.Equal(3, table.Columns.Count);

                var idCol = table.Columns.Single(c => c.Name == "Id");
                Assert.Equal("INT64", idCol.StoreType);
                Assert.False(idCol.IsNullable);

                var metadataCol = table.Columns.Single(c => c.Name == "Metadata");
                Assert.Equal("JSON", metadataCol.StoreType);
                Assert.True(metadataCol.IsNullable);
                Assert.True((bool?)metadataCol["BigQuery:IsJsonColumn"] ?? false);

                var settingsCol = table.Columns.Single(c => c.Name == "Settings");
                Assert.Equal("JSON", settingsCol.StoreType);
                Assert.False(settingsCol.IsNullable);
                Assert.True((bool?)settingsCol["BigQuery:IsJsonColumn"] ?? false);
            },
            "DROP TABLE JsonTable;");

    #endregion

    #region Geography

    [ConditionalFact]
    public void Create_geography_columns()
        => TestWithNts(
            @"
CREATE TABLE GeographyTable (
    Id INT64 NOT NULL,
    Location GEOGRAPHY,
    Area GEOGRAPHY NOT NULL
);",
            Enumerable.Empty<string>(),
            Enumerable.Empty<string>(),
            dbModel =>
            {
                var table = dbModel.Tables.Single();
                Assert.Equal("GeographyTable", table.Name);
                Assert.Equal(3, table.Columns.Count);

                var idCol = table.Columns.Single(c => c.Name == "Id");
                Assert.Equal("INT64", idCol.StoreType);
                Assert.False(idCol.IsNullable);

                var locationCol = table.Columns.Single(c => c.Name == "Location");
                Assert.Equal("GEOGRAPHY", locationCol.StoreType);
                Assert.True(locationCol.IsNullable);

                var areaCol = table.Columns.Single(c => c.Name == "Area");
                Assert.Equal("GEOGRAPHY", areaCol.StoreType);
                Assert.False(areaCol.IsNullable);
            },
            "DROP TABLE GeographyTable;");

    private void TestWithNts(
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
            new BigQueryNetTopologySuiteDesignTimeServices().ConfigureDesignTimeServices(services);

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