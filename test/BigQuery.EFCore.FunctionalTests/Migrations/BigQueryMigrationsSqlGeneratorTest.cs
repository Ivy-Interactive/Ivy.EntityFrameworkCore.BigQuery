using Ivy.EntityFrameworkCore.BigQuery.Extensions;
using Ivy.EntityFrameworkCore.BigQuery.FunctionalTests.TestUtilities;
using Ivy.EntityFrameworkCore.BigQuery.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.Extensions.DependencyInjection;
using Ivy.EntityFrameworkCore.BigQuery.Metadata.Internal;
using Microsoft.EntityFrameworkCore.Migrations.Operations;

namespace Ivy.EntityFrameworkCore.BigQuery.FunctionalTests.Migrations
{
    public class BigQueryMigrationsSqlGeneratorTest() : MigrationsSqlGeneratorTestBase(
       BigQueryTestHelpers.Instance,
        new ServiceCollection(),
        BigQueryTestHelpers.Instance.AddProviderOptions(
            ((IRelationalDbContextOptionsBuilderInfrastructure)
                new BigQueryDbContextOptionsBuilder(new DbContextOptionsBuilder()))
            .OptionsBuilder).Options)
    {
        protected override string GetGeometryCollectionStoreType() => "GEOGRAPHY";

        [ConditionalFact]
        public void EnsureSchema_generates_create_schema_if_not_exists()
        {
            Generate(new EnsureSchemaOperation { Name = "sales" });

            AssertSql(
                """
CREATE SCHEMA IF NOT EXISTS `sales`;
""");
        }

        [ConditionalFact]
        public void DropSchema_generates_drop_schema_if_exists()
        {
            Generate(new DropSchemaOperation { Name = "sales" });

            AssertSql(
                """
DROP SCHEMA IF EXISTS `sales`;
""");
        }

        [ConditionalFact]
        public void CreateTable_with_pk_and_fk_not_enforced()
        {
            Generate(
                new CreateTableOperation
                {
                    Name = "People",
                    Columns =
                    {
                        new AddColumnOperation { Name = "Id", Table = "People", ClrType = typeof(long), ColumnType = "INT64", IsNullable = false },
                        new AddColumnOperation { Name = "EmployerId", Table = "People", ClrType = typeof(long), ColumnType = "INT64", IsNullable = true },
                    },
                    PrimaryKey = new AddPrimaryKeyOperation { Columns = ["Id"] },
                    ForeignKeys =
                    {
                        new AddForeignKeyOperation
                        {
                            Table = "People",
                            Columns = ["EmployerId"],
                            PrincipalTable = "Companies",
                            PrincipalColumns = ["Id"]
                        }
                    }
                });

            AssertSql(
                """
CREATE TABLE `People` (
    `Id` INT64 NOT NULL,
    `EmployerId` INT64,
    PRIMARY KEY (`Id`) NOT ENFORCED,
    FOREIGN KEY (`EmployerId`) REFERENCES `Companies` (`Id`) NOT ENFORCED
);
""");
        }

        [ConditionalFact]
        public void AddColumn_with_datetime_default_generates_literal()
        {
            Generate(
                new AddColumnOperation
                {
                    Table = "History",
                    Name = "Event",
                    ClrType = typeof(DateTime),
                    ColumnType = "DATETIME",
                    IsNullable = false,
                    DefaultValue = new DateTime(2025, 1, 2, 3, 4, 5)
                });

            AssertSql(
                """
ALTER TABLE `History` ADD COLUMN `Event` DATETIME NOT NULL DEFAULT DATETIME '2025-01-02 03:04:05.000000';
""");
        }

        [ConditionalFact]
        public void Rename_table_and_column_generate_statements()
        {
            Generate(
                new RenameTableOperation { Name = "People", NewName = "Person" },
                new RenameColumnOperation { Table = "Person", Name = "FullName", NewName = "Name" });

            AssertSql(
                """
ALTER TABLE `People` RENAME TO `Person`;
GO

ALTER TABLE `Person` RENAME COLUMN `FullName` TO `Name`;
""");
        }

        [ConditionalFact]
        public void AlterColumn_generates_set_data_type_and_drop_not_null()
        {
            Generate(
                new AlterColumnOperation
                {
                    Table = "T",
                    Name = "C",
                    ClrType = typeof(string),
                    ColumnType = "STRING",
                    IsNullable = true,
                    OldColumn = new AddColumnOperation
                    {
                        Table = "T",
                        Name = "C",
                        ClrType = typeof(string),
                        ColumnType = "BYTES",
                        IsNullable = false
                    }
                });

            AssertSql(
                """
ALTER TABLE `T` ALTER COLUMN `C` SET DATA TYPE STRING;
GO

ALTER TABLE `T` ALTER COLUMN `C` DROP NOT NULL;
""");
        }

        [ConditionalFact]
        public void DropPrimaryKey_generates_statement()
        {
            Generate(new DropPrimaryKeyOperation { Table = "T" });

            AssertSql(
                """
ALTER TABLE `T` DROP PRIMARY KEY;
""");
        }

        [ConditionalFact]
        public void DropForeignKey_generates_statement()
        {
            Generate(new DropForeignKeyOperation { Table = "T", Name = "FK_T_Ref" });

            AssertSql(
                """
ALTER TABLE `T` DROP CONSTRAINT `FK_T_Ref`;
""");
        }

        [ConditionalFact]
        public void Create_search_index_on_columns()
        {
            var op = new CreateIndexOperation
            {
                Name = "SIDX_People_Name",
                Table = "People",
                Columns = ["Name", "Notes"],
                [BigQueryAnnotationNames.SearchIndex] = true,
                [BigQueryAnnotationNames.IfNotExists] = true,
                [BigQueryAnnotationNames.IndexOptions] = "analyzer='LOG_ANALYZER'",
                [BigQueryAnnotationNames.IndexColumnOptions] = new Dictionary<string, string>
                {
                    ["Notes"] = "index_granularity='GLOBAL'"
                },
            };
           

            Generate(op);

            AssertSql(
                """
CREATE SEARCH INDEX IF NOT EXISTS `SIDX_People_Name` ON `People`(`Name`, `Notes` OPTIONS(index_granularity='GLOBAL')) OPTIONS(analyzer='LOG_ANALYZER');
""");
        }

        [ConditionalFact]
        public void Create_search_index_all_columns_with_column_options()
        {
            var op = new CreateIndexOperation
            {
                Name = "SIDX_All",
                Table = "Docs",
                Columns = Array.Empty<string>()
            };
            op[BigQueryAnnotationNames.SearchIndex] = true;
            op[BigQueryAnnotationNames.AllColumns] = true;
            op[BigQueryAnnotationNames.IndexColumnOptions] = new Dictionary<string, string>
            {
                ["Title"] = "index_granularity='COLUMN'"
            };

            Generate(op);

            AssertSql(
                """
CREATE SEARCH INDEX `SIDX_All` ON `Docs`(ALL COLUMNS WITH COLUMN OPTIONS(`Title` OPTIONS(index_granularity='COLUMN')));
""");
        }

        [ConditionalFact]        
        public void Drop_search_index_generates_statement()
        {
            var op = new DropIndexOperation { Name = "SIDX_All", Table = "Docs" };
            op[BigQueryAnnotationNames.SearchIndex] = true;
            op[BigQueryAnnotationNames.IfExists] = true;

            Generate(op);

            AssertSql(
                """
DROP SEARCH INDEX IF EXISTS `SIDX_All` ON `Docs`;
""");
        }

        [ConditionalFact]
        public void AddUniqueConstraint_throws_not_supported()
        {
            var ex = Assert.Throws<NotSupportedException>(() => Generate(new AddUniqueConstraintOperation { Table = "T", Columns = ["C"] }));
            Assert.Equal("UNIQUE constraints are not supported by BigQuery.", ex.Message);
        }

        [ConditionalFact]
        public void CreateTable_with_create_or_replace()
        {
            var operation = new CreateTableOperation
            {
                Name = "TestTable",
                Columns =
                {
                    new AddColumnOperation
                    {
                        Name = "Id",
                        Table = "TestTable",
                        ClrType = typeof(int),
                        ColumnType = "INT64",
                        IsNullable = false
                    }
                },
                PrimaryKey = new AddPrimaryKeyOperation { Columns = ["Id"] }
            };
            operation[BigQueryAnnotationNames.CreateOrReplace] = true;

            Generate(operation);

            AssertSql(
                """
CREATE OR REPLACE TABLE `TestTable` (
    `Id` INT64 NOT NULL,
    PRIMARY KEY (`Id`) NOT ENFORCED
);
""");
        }

        [ConditionalFact]
        public void CreateTable_with_if_not_exists()
        {
            var operation = new CreateTableOperation
            {
                Name = "TestTable",
                Columns =
                {
                    new AddColumnOperation
                    {
                        Name = "Id",
                        Table = "TestTable",
                        ClrType = typeof(int),
                        ColumnType = "INT64",
                        IsNullable = false
                    }
                },
                PrimaryKey = new AddPrimaryKeyOperation { Columns = ["Id"] }
            };
            operation[BigQueryAnnotationNames.IfNotExists] = true;

            Generate(operation);

            AssertSql(
                """
CREATE TABLE IF NOT EXISTS `TestTable` (
    `Id` INT64 NOT NULL,
    PRIMARY KEY (`Id`) NOT ENFORCED
);
""");
        }

        [ConditionalFact]
        public void CreateTable_with_temp_table()
        {
            var operation = new CreateTableOperation
            {
                Name = "TestTable",
                Columns =
                {
                    new AddColumnOperation
                    {
                        Name = "Id",
                        Table = "TestTable",
                        ClrType = typeof(int),
                        ColumnType = "INT64",
                        IsNullable = false
                    }
                },
                PrimaryKey = new AddPrimaryKeyOperation { Columns = ["Id"] }
            };
            operation[BigQueryAnnotationNames.TempTable] = true;

            Generate(operation);

            AssertSql(
                """
CREATE TEMP TABLE `TestTable` (
    `Id` INT64 NOT NULL,
    PRIMARY KEY (`Id`) NOT ENFORCED
);
""");
        }

        [ConditionalFact]
        public void CreateTable_with_temp_table_if_not_exists()
        {
            var operation = new CreateTableOperation
            {
                Name = "TestTable",
                Columns =
                {
                    new AddColumnOperation
                    {
                        Name = "Id",
                        Table = "TestTable",
                        ClrType = typeof(int),
                        ColumnType = "INT64",
                        IsNullable = false
                    }
                },
                PrimaryKey = new AddPrimaryKeyOperation { Columns = ["Id"] }
            };
            operation[BigQueryAnnotationNames.TempTable] = true;
            operation[BigQueryAnnotationNames.IfNotExists] = true;

            Generate(operation);

            AssertSql(
                """
CREATE TEMP TABLE IF NOT EXISTS `TestTable` (
    `Id` INT64 NOT NULL,
    PRIMARY KEY (`Id`) NOT ENFORCED
);
""");
        }

        [ConditionalFact]
        public void CreateTable_with_create_or_replace_temp_table()
        {
            var operation = new CreateTableOperation
            {
                Name = "TestTable",
                Columns =
                {
                    new AddColumnOperation
                    {
                        Name = "Id",
                        Table = "TestTable",
                        ClrType = typeof(int),
                        ColumnType = "INT64",
                        IsNullable = false
                    }
                },
                PrimaryKey = new AddPrimaryKeyOperation { Columns = ["Id"] }
            };
            operation[BigQueryAnnotationNames.CreateOrReplace] = true;
            operation[BigQueryAnnotationNames.TempTable] = true;

            Generate(operation);

            AssertSql(
                """
CREATE OR REPLACE TEMP TABLE `TestTable` (
    `Id` INT64 NOT NULL,
    PRIMARY KEY (`Id`) NOT ENFORCED
);
""");
        }

        [ConditionalFact]
        public void CreateTable_with_create_or_replace_and_if_not_exists_throws_exception()
        {
            var operation = new CreateTableOperation
            {
                Name = "TestTable",
                Columns =
                {
                    new AddColumnOperation
                    {
                        Name = "Id",
                        Table = "TestTable",
                        ClrType = typeof(int),
                        ColumnType = "INT64",
                        IsNullable = false
                    }
                },
                PrimaryKey = new AddPrimaryKeyOperation { Columns = ["Id"] }
            };
            operation[BigQueryAnnotationNames.CreateOrReplace] = true;
            operation[BigQueryAnnotationNames.IfNotExists] = true;

            var ex = Assert.Throws<InvalidOperationException>(() => Generate(operation));
            Assert.Equal("CREATE OR REPLACE and IF NOT EXISTS cannot be used together in BigQuery.", ex.Message);
        }


        [ConditionalFact]
        public override void AddColumnOperation_without_column_type()
        {
            Generate(
                new AddColumnOperation
                {
                    Table = "People",
                    Name = "Alias",
                    ClrType = typeof(string),
                    IsNullable = false
                });

            AssertSql(
                """
ALTER TABLE `People` ADD COLUMN `Alias` STRING NOT NULL;
""");
        }

        [ConditionalFact(Skip = "STRING has no ANSI/Unicode distinction")]
        public override void AddColumnOperation_with_unicode_overridden() => base.AddColumnOperation_with_unicode_overridden();

        [ConditionalFact(Skip = "STRING has no ANSI/Unicode distinction")]
        public override void AddColumnOperation_with_unicode_no_model() => base.AddColumnOperation_with_unicode_no_model();

        [ConditionalFact(Skip = "STRING is not fixed length")]
        public override void AddColumnOperation_with_fixed_length_no_model() => base.AddColumnOperation_with_fixed_length_no_model();

        [ConditionalFact(Skip = "STRING length not enforced")]
        public override void AddColumnOperation_with_maxLength_overridden() => base.AddColumnOperation_with_maxLength_overridden();

        [ConditionalFact(Skip = "STRING length not enforced")]
        public override void AddColumnOperation_with_maxLength_no_model() => base.AddColumnOperation_with_maxLength_no_model();

        [ConditionalFact(Skip = "BigQuery precision/scale semantics differ")]
        public override void AddColumnOperation_with_precision_and_scale_overridden() => base.AddColumnOperation_with_precision_and_scale_overridden();

        [ConditionalFact(Skip = "BigQuery precision/scale semantics differ")]
        public override void AddColumnOperation_with_precision_and_scale_no_model() => base.AddColumnOperation_with_precision_and_scale_no_model();

        [ConditionalFact(Skip = "Base test expects provider-specific exception; not applicable to BigQuery.")]
        public override void AddForeignKeyOperation_without_principal_columns() => base.AddForeignKeyOperation_without_principal_columns();

        [ConditionalFact(Skip = "BigQuery requires explicit column types in migrations")]
        public override void AlterColumnOperation_without_column_type() => base.AlterColumnOperation_without_column_type();

        [ConditionalFact]
        public override void RenameTableOperation_legacy()
        {
            Generate(new RenameTableOperation { Name = "People", NewName = "Person" });

            AssertSql(
                """
ALTER TABLE `People` RENAME TO `Person`;
""");
        }

        [ConditionalFact]
        public override void RenameTableOperation()
        {
            Generate(new RenameTableOperation { Name = "People", NewName = "Person" });

            AssertSql(
                """
ALTER TABLE `People` RENAME TO `Person`;
""");
        }

        [ConditionalFact(Skip = "Todo sql generator")]
        public override void InsertDataOperation_all_args_spatial() => base.InsertDataOperation_all_args_spatial();

        [ConditionalFact(Skip = "Todo sql generator")]
        public override void InsertDataOperation_required_args() => base.InsertDataOperation_required_args();

        [ConditionalFact(Skip = "Todo sql generator")]
        public override void InsertDataOperation_required_args_composite() => base.InsertDataOperation_required_args_composite();

        [ConditionalFact(Skip = "Todo sql generator")]
        public override void InsertDataOperation_required_args_multiple_rows() => base.InsertDataOperation_required_args_multiple_rows();

        [ConditionalFact(Skip = "BigQuery supports all scalar types; provider does not restrict here.")]
        public override void InsertDataOperation_throws_for_unsupported_column_types() => base.InsertDataOperation_throws_for_unsupported_column_types();

        [ConditionalFact(Skip = "Todo sql generator")]
        public override void DeleteDataOperation_all_args() => base.DeleteDataOperation_all_args();

        [ConditionalFact(Skip = "Todo sql generator")]
        public override void DeleteDataOperation_all_args_composite() => base.DeleteDataOperation_all_args_composite();

        [ConditionalFact(Skip = "Todo sql generator")]
        public override void DeleteDataOperation_required_args() => base.DeleteDataOperation_required_args();

        [ConditionalFact(Skip = "Todo sql generator")]
        public override void DeleteDataOperation_required_args_composite() => base.DeleteDataOperation_required_args_composite();

        [ConditionalFact(Skip = "Todo sql generator")]
        public override void UpdateDataOperation_all_args() => base.UpdateDataOperation_all_args();

        [ConditionalFact(Skip = "Todo sql generator")]
        public override void UpdateDataOperation_all_args_composite() => base.UpdateDataOperation_all_args_composite();

        [ConditionalFact(Skip = "Todo sql generator")]
        public override void UpdateDataOperation_all_args_composite_multi() => base.UpdateDataOperation_all_args_composite_multi();

        [ConditionalFact(Skip = "Todo sql generator")]
        public override void UpdateDataOperation_all_args_multi() => base.UpdateDataOperation_all_args_multi();

        [ConditionalFact(Skip = "Todo sql generator")]
        public override void UpdateDataOperation_required_args() => base.UpdateDataOperation_required_args();

        [ConditionalFact(Skip = "Todo sql generator")]
        public override void UpdateDataOperation_required_args_multiple_rows() => base.UpdateDataOperation_required_args_multiple_rows();

        [ConditionalFact(Skip = "Todo sql generator")]
        public override void UpdateDataOperation_required_args_composite() => base.UpdateDataOperation_required_args_composite();

        [ConditionalFact(Skip = "Todo sql generator")]
        public override void UpdateDataOperation_required_args_composite_multi() => base.UpdateDataOperation_required_args_composite_multi();

        [ConditionalFact(Skip = "Todo sql generator")]
        public override void UpdateDataOperation_required_args_multi() => base.UpdateDataOperation_required_args_multi();

        [InlineData(true)]
        [InlineData(false)]
        public override void DefaultValue_with_line_breaks(bool isUnicode) => base.DefaultValue_with_line_breaks(isUnicode);

        [InlineData(true)]
        [InlineData(false)]
        public override void DefaultValue_with_line_breaks_2(bool isUnicode) => base.DefaultValue_with_line_breaks_2(isUnicode);

        [ConditionalTheory(Skip = "Sequences are not supported by BigQuery.")]
        [InlineData(null)]
        [InlineData(1L)]
        public override void Sequence_restart_operation(long? startsAt) => base.Sequence_restart_operation(startsAt);
    }
}
