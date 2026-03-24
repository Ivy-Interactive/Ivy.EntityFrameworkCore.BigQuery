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

    #region Location

    [ConditionalFact]
    public void Scaffolding_captures_dataset_location()
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
            new DatabaseModelFactoryOptions([], []));

        Assert.NotNull(databaseModel);

        // Location should be captured from INFORMATION_SCHEMA.SCHEMATA
        var location = databaseModel["BigQuery:Location"] as string;
        Assert.NotNull(location);
        Assert.NotEmpty(location);
    }

    #endregion

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

                Assert.Single(table.Columns, c => c.Name == "Id");
                Assert.Single(table.Columns, c => c.Name == "Name");
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
        var pk = dbModel.Tables.Single().PrimaryKey!;

        Assert.Equal(Fixture.TestStore.DatasetName, pk.Table!.Schema);
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

    #region Filtering

    [ConditionalFact]
    public void Filters_tables_by_name()
        => Test(
            """
CREATE TABLE IncludedTable (Id INT64 NOT NULL);
CREATE TABLE ExcludedTable (Id INT64 NOT NULL);
CREATE TABLE AnotherExcluded (Id INT64 NOT NULL);
""",
            ["IncludedTable"],
            [],
            dbModel =>
            {
                var table = Assert.Single(dbModel.Tables);
                Assert.Equal("IncludedTable", table.Name);
            },
            """
DROP TABLE IncludedTable;
DROP TABLE ExcludedTable;
DROP TABLE AnotherExcluded;
""");

    [ConditionalFact]
    public void Filters_multiple_tables_by_name()
        => Test(
            """
CREATE TABLE Alpha (Id INT64 NOT NULL);
CREATE TABLE Beta (Id INT64 NOT NULL);
CREATE TABLE Gamma (Id INT64 NOT NULL);
""",
            ["Alpha", "Gamma"],
            [],
            dbModel =>
            {
                Assert.Equal(2, dbModel.Tables.Count);
                Assert.Contains(dbModel.Tables, t => t.Name == "Alpha");
                Assert.Contains(dbModel.Tables, t => t.Name == "Gamma");
                Assert.DoesNotContain(dbModel.Tables, t => t.Name == "Beta");
            },
            """
DROP TABLE Alpha;
DROP TABLE Beta;
DROP TABLE Gamma;
""");

    #endregion

    #region Views

    [ConditionalFact]
    public void Scaffolds_view_columns()
        => Test(
            """
CREATE VIEW ProductSummary AS
SELECT 1 AS ProductId, 'Widget' AS ProductName, 99.99 AS Price;
""",
            [],
            [],
            dbModel =>
            {
                var view = Assert.Single(dbModel.Tables);
                Assert.IsType<DatabaseView>(view);
                Assert.Equal("ProductSummary", view.Name);
                Assert.Equal(3, view.Columns.Count);

                Assert.Contains(view.Columns, c => c.Name == "ProductId");
                Assert.Contains(view.Columns, c => c.Name == "ProductName");
                Assert.Contains(view.Columns, c => c.Name == "Price");

                // Views don't have primary keys
                Assert.Null(view.PrimaryKey);
            },
            "DROP VIEW ProductSummary;");

    #endregion

    #region ColumnFacets

    [ConditionalFact]
    public void Numeric_columns_preserve_precision_and_scale()
        => Test(
            """
CREATE TABLE PrecisionTable (
    Id INT64 NOT NULL,
    DefaultNumeric NUMERIC,
    PreciseNumeric NUMERIC(10, 4),
    DefaultBigNumeric BIGNUMERIC,
    PreciseBigNumeric BIGNUMERIC(30, 10)
);
""",
            [],
            [],
            dbModel =>
            {
                var table = dbModel.Tables.Single();
                var columns = table.Columns;

                var defaultNum = columns.Single(c => c.Name == "DefaultNumeric");
                Assert.Equal("NUMERIC", defaultNum.StoreType);

                var preciseNum = columns.Single(c => c.Name == "PreciseNumeric");
                Assert.Equal("NUMERIC(10, 4)", preciseNum.StoreType);

                var defaultBigNum = columns.Single(c => c.Name == "DefaultBigNumeric");
                Assert.Equal("BIGNUMERIC", defaultBigNum.StoreType);

                var preciseBigNum = columns.Single(c => c.Name == "PreciseBigNumeric");
                Assert.Equal("BIGNUMERIC(30, 10)", preciseBigNum.StoreType);
            },
            "DROP TABLE PrecisionTable;");

    [ConditionalFact]
    public void String_and_bytes_columns_preserve_max_length()
        => Test(
            """
CREATE TABLE LengthConstrainedTable (
    Id INT64 NOT NULL,
    UnboundedString STRING,
    BoundedString STRING(100),
    UnboundedBytes BYTES,
    BoundedBytes BYTES(256)
);
""",
            [],
            [],
            dbModel =>
            {
                var table = dbModel.Tables.Single();
                var columns = table.Columns;

                Assert.Equal("STRING", columns.Single(c => c.Name == "UnboundedString").StoreType);
                Assert.Equal("STRING(100)", columns.Single(c => c.Name == "BoundedString").StoreType);
                Assert.Equal("BYTES", columns.Single(c => c.Name == "UnboundedBytes").StoreType);
                Assert.Equal("BYTES(256)", columns.Single(c => c.Name == "BoundedBytes").StoreType);
            },
            "DROP TABLE LengthConstrainedTable;");

    [ConditionalFact]
    public void All_bigquery_types_scaffold_correctly()
        => Test(
            """
CREATE TABLE AllTypesTable (
    ColInt64 INT64,
    ColFloat64 FLOAT64,
    ColBool BOOL,
    ColString STRING,
    ColBytes BYTES,
    ColDate DATE,
    ColTime TIME,
    ColDateTime DATETIME,
    ColTimestamp TIMESTAMP,
    ColNumeric NUMERIC,
    ColBigNumeric BIGNUMERIC,
    ColJson JSON,
    ColGeography GEOGRAPHY
);
""",
            [],
            [],
            dbModel =>
            {
                var columns = dbModel.Tables.Single().Columns;

                Assert.Equal("INT64", columns.Single(c => c.Name == "ColInt64").StoreType);
                Assert.Equal("FLOAT64", columns.Single(c => c.Name == "ColFloat64").StoreType);
                Assert.Equal("BOOL", columns.Single(c => c.Name == "ColBool").StoreType);
                Assert.Equal("STRING", columns.Single(c => c.Name == "ColString").StoreType);
                Assert.Equal("BYTES", columns.Single(c => c.Name == "ColBytes").StoreType);
                Assert.Equal("DATE", columns.Single(c => c.Name == "ColDate").StoreType);
                Assert.Equal("TIME", columns.Single(c => c.Name == "ColTime").StoreType);
                Assert.Equal("DATETIME", columns.Single(c => c.Name == "ColDateTime").StoreType);
                Assert.Equal("TIMESTAMP", columns.Single(c => c.Name == "ColTimestamp").StoreType);
                Assert.Equal("NUMERIC", columns.Single(c => c.Name == "ColNumeric").StoreType);
                Assert.Equal("BIGNUMERIC", columns.Single(c => c.Name == "ColBigNumeric").StoreType);
                Assert.Equal("JSON", columns.Single(c => c.Name == "ColJson").StoreType);
                Assert.Equal("GEOGRAPHY", columns.Single(c => c.Name == "ColGeography").StoreType);
            },
            "DROP TABLE AllTypesTable;");

    [ConditionalFact]
    public void Column_nullability_is_captured()
        => Test(
            """
CREATE TABLE NullabilityTable (
    RequiredId INT64 NOT NULL,
    OptionalValue INT64,
    RequiredName STRING NOT NULL,
    OptionalDescription STRING
);
""",
            [],
            [],
            dbModel =>
            {
                var columns = dbModel.Tables.Single().Columns;

                Assert.False(columns.Single(c => c.Name == "RequiredId").IsNullable);
                Assert.True(columns.Single(c => c.Name == "OptionalValue").IsNullable);
                Assert.False(columns.Single(c => c.Name == "RequiredName").IsNullable);
                Assert.True(columns.Single(c => c.Name == "OptionalDescription").IsNullable);
            },
            "DROP TABLE NullabilityTable;");

    [ConditionalFact]
    public void Default_values_are_captured()
        => Test(
            """
CREATE TABLE DefaultValuesTable (
    Id INT64 NOT NULL,
    Status STRING DEFAULT 'pending',
    CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    Counter INT64 DEFAULT 0
);
""",
            [],
            [],
            dbModel =>
            {
                var columns = dbModel.Tables.Single().Columns;

                var statusCol = columns.Single(c => c.Name == "Status");
                Assert.NotNull(statusCol.DefaultValueSql);
                Assert.Contains("pending", statusCol.DefaultValueSql);

                var createdAtCol = columns.Single(c => c.Name == "CreatedAt");
                Assert.NotNull(createdAtCol.DefaultValueSql);

                var counterCol = columns.Single(c => c.Name == "Counter");
                Assert.NotNull(counterCol.DefaultValueSql);
            },
            "DROP TABLE DefaultValuesTable;");

    #endregion

    #region PrimaryKeyFacets

    [ConditionalFact]
    public void Composite_primary_key_columns_are_ordered()
        => Test(
            """
CREATE TABLE CompositeKeyTable (
    TenantId INT64 NOT NULL,
    UserId INT64 NOT NULL,
    RecordId INT64 NOT NULL,
    Data STRING,
    PRIMARY KEY (TenantId, UserId, RecordId) NOT ENFORCED
);
""",
            [],
            [],
            dbModel =>
            {
                var pk = dbModel.Tables.Single().PrimaryKey;
                Assert.NotNull(pk);

                var keyColumns = pk.Columns.Select(c => c.Name).ToList();
                Assert.Equal(["TenantId", "UserId", "RecordId"], keyColumns);
            },
            "DROP TABLE CompositeKeyTable;");

    #endregion

    #region ForeignKeyFacets

    [ConditionalFact]
    public void Composite_foreign_key_is_scaffolded()
        => Test(
            """
CREATE TABLE ParentWithCompositeKey (
    KeyPartA INT64 NOT NULL,
    KeyPartB INT64 NOT NULL,
    PRIMARY KEY (KeyPartA, KeyPartB) NOT ENFORCED
);

CREATE TABLE ChildWithCompositeFK (
    Id INT64 NOT NULL,
    RefA INT64,
    RefB INT64,
    PRIMARY KEY (Id) NOT ENFORCED,
    FOREIGN KEY (RefA, RefB) REFERENCES ParentWithCompositeKey(KeyPartA, KeyPartB) NOT ENFORCED
);
""",
            [],
            [],
            dbModel =>
            {
                var childTable = dbModel.Tables.Single(t => t.Name == "ChildWithCompositeFK");
                var fk = Assert.Single(childTable.ForeignKeys);

                Assert.Equal("ParentWithCompositeKey", fk.PrincipalTable.Name);
                Assert.Equal(["RefA", "RefB"], fk.Columns.Select(c => c.Name).ToList());
                Assert.Equal(["KeyPartA", "KeyPartB"], fk.PrincipalColumns.Select(c => c.Name).ToList());
            },
            """
DROP TABLE ChildWithCompositeFK;
DROP TABLE ParentWithCompositeKey;
""");

    [ConditionalFact]
    public void Multiple_foreign_keys_on_single_table()
        => Test(
            """
CREATE TABLE Authors (
    AuthorId INT64 NOT NULL,
    PRIMARY KEY (AuthorId) NOT ENFORCED
);

CREATE TABLE Categories (
    CategoryId INT64 NOT NULL,
    PRIMARY KEY (CategoryId) NOT ENFORCED
);

CREATE TABLE Articles (
    ArticleId INT64 NOT NULL,
    AuthorId INT64,
    CategoryId INT64,
    PRIMARY KEY (ArticleId) NOT ENFORCED,
    FOREIGN KEY (AuthorId) REFERENCES Authors(AuthorId) NOT ENFORCED,
    FOREIGN KEY (CategoryId) REFERENCES Categories(CategoryId) NOT ENFORCED
);
""",
            [],
            [],
            dbModel =>
            {
                var articles = dbModel.Tables.Single(t => t.Name == "Articles");
                Assert.Equal(2, articles.ForeignKeys.Count);

                var authorFk = articles.ForeignKeys.Single(fk => fk.PrincipalTable.Name == "Authors");
                Assert.Equal(["AuthorId"], authorFk.Columns.Select(c => c.Name).ToList());

                var categoryFk = articles.ForeignKeys.Single(fk => fk.PrincipalTable.Name == "Categories");
                Assert.Equal(["CategoryId"], categoryFk.Columns.Select(c => c.Name).ToList());
            },
            """
DROP TABLE Articles;
DROP TABLE Categories;
DROP TABLE Authors;
""");

    [ConditionalFact]
    public void Foreign_key_constraint_name_is_captured()
        => Test(
            """
CREATE TABLE Orders (
    OrderId INT64 NOT NULL,
    PRIMARY KEY (OrderId) NOT ENFORCED
);

CREATE TABLE OrderItems (
    ItemId INT64 NOT NULL,
    OrderId INT64,
    PRIMARY KEY (ItemId) NOT ENFORCED,
    CONSTRAINT FK_OrderItems_Orders FOREIGN KEY (OrderId) REFERENCES Orders(OrderId) NOT ENFORCED
);
""",
            [],
            [],
            dbModel =>
            {
                var orderItems = dbModel.Tables.Single(t => t.Name == "OrderItems");
                var fk = Assert.Single(orderItems.ForeignKeys);

                Assert.Equal("FK_OrderItems_Orders", fk.Name);
                Assert.Equal("Orders", fk.PrincipalTable.Name);
            },
            """
DROP TABLE OrderItems;
DROP TABLE Orders;
""");

    [ConditionalFact]
    public void All_foreign_keys_have_no_action_delete_behavior()
        => Test(
            """
CREATE TABLE Referenced (
    Id INT64 NOT NULL,
    PRIMARY KEY (Id) NOT ENFORCED
);

CREATE TABLE Referencing (
    Id INT64 NOT NULL,
    RefId INT64,
    PRIMARY KEY (Id) NOT ENFORCED,
    FOREIGN KEY (RefId) REFERENCES Referenced(Id) NOT ENFORCED
);
""",
            [],
            [],
            dbModel =>
            {
                var fk = dbModel.Tables.Single(t => t.Name == "Referencing").ForeignKeys.Single();

                // BigQuery foreign keys are NOT ENFORCED, so they always map to NoAction
                Assert.Equal(ReferentialAction.NoAction, fk.OnDelete);
            },
            """
DROP TABLE Referencing;
DROP TABLE Referenced;
""");

    #endregion

    #region Comments

    //Todo
//    [ConditionalFact]
//    public void Table_and_column_descriptions_are_captured()
//        => Test(
//            """
//CREATE TABLE DescribedTable (
//    Id INT64 NOT NULL OPTIONS(description='Unique identifier for each record'),
//    Name STRING OPTIONS(description='Display name of the entity')
//)
//OPTIONS(description='A table with descriptions for testing scaffolding');
//""",
//            [],
//            [],
//            dbModel =>
//            {
//                var table = dbModel.Tables.Single();
//                Assert.Equal("A table with descriptions for testing scaffolding", table.Comment);

//                var idCol = table.Columns.Single(c => c.Name == "Id");
//                Assert.Equal("Unique identifier for each record", idCol.Comment);

//                var nameCol = table.Columns.Single(c => c.Name == "Name");
//                Assert.Equal("Display name of the entity", nameCol.Comment);
//            },
//            "DROP TABLE DescribedTable;");

    #endregion

    #region EdgeCases

    [ConditionalFact]
    public void Table_with_no_primary_key_is_scaffolded()
        => Test(
            """
CREATE TABLE HeapTable (
    Col1 INT64,
    Col2 STRING
);
""",
            [],
            [],
            dbModel =>
            {
                var table = dbModel.Tables.Single();
                Assert.Equal("HeapTable", table.Name);
                Assert.Null(table.PrimaryKey);
                Assert.Equal(2, table.Columns.Count);
            },
            "DROP TABLE HeapTable;");

    [ConditionalFact]
    public void Empty_table_is_scaffolded()
        => Test(
            """
CREATE TABLE EmptyStructure (
    Placeholder INT64
);
""",
            [],
            [],
            dbModel =>
            {
                var table = dbModel.Tables.Single();
                Assert.Equal("EmptyStructure", table.Name);
                Assert.Single(table.Columns);
            },
            "DROP TABLE EmptyStructure;");

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