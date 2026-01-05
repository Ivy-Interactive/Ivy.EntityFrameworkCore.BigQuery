using Microsoft.EntityFrameworkCore;
using Ivy.EntityFrameworkCore.BigQuery.Extensions;
using Ivy.EntityFrameworkCore.BigQuery.Metadata.Internal;

#pragma warning disable EF1001 // Internal EF Core API usage.

namespace Ivy.EntityFrameworkCore.BigQuery.Migrations;

public class BigQueryFluentApiExtensionsTest
{
    [ConditionalFact]
    public void FluentApiExtensions_configure_search_index_correctly()
    {
        var modelBuilder = new ModelBuilder();
        
        modelBuilder.Entity<Document>(entity =>
        {
            entity.HasIndex(e => new { e.Title, e.Content })
                .HasBigQuerySearchIndex(ifNotExists: true)
                .HasBigQuerySearchIndexOptions("analyzer='LOG_ANALYZER'")
                .HasBigQuerySearchIndexColumnOptions("Content", "index_granularity='GLOBAL'");
        });

        var model = modelBuilder.FinalizeModel();
        var index = model.FindEntityType(typeof(Document))!.GetIndexes().First();

        Assert.True(index[BigQueryAnnotationNames.SearchIndex] as bool?);
        Assert.True(index[BigQueryAnnotationNames.IfNotExists] as bool?);
        Assert.Equal("analyzer='LOG_ANALYZER'", index[BigQueryAnnotationNames.IndexOptions] as string);
        
        var columnOptions = index[BigQueryAnnotationNames.IndexColumnOptions] as IDictionary<string, string>;
        Assert.NotNull(columnOptions);
        Assert.Equal("index_granularity='GLOBAL'", columnOptions["Content"]);
    }

    [ConditionalFact]
    public void FluentApiExtensions_configure_all_columns_search_index()
    {
        var modelBuilder = new ModelBuilder();
        
        modelBuilder.Entity<Document>(entity =>
        {
            entity.HasIndex(e => e.Title, "SIDX_All")
                .HasBigQuerySearchIndexOnAllColumns(ifNotExists: true)
                .HasBigQuerySearchIndexColumnOptions(new Dictionary<string, string>
                {
                    ["Title"] = "index_granularity='COLUMN'"
                });
        });

        var model = modelBuilder.FinalizeModel();
        var index = model.FindEntityType(typeof(Document))!.GetIndexes().First();

        Assert.True(index[BigQueryAnnotationNames.SearchIndex] as bool?);
        Assert.True(index[BigQueryAnnotationNames.AllColumns] as bool?);
        Assert.True(index[BigQueryAnnotationNames.IfNotExists] as bool?);
        
        var columnOptions = index[BigQueryAnnotationNames.IndexColumnOptions] as IDictionary<string, string>;
        Assert.NotNull(columnOptions);
        Assert.Equal("index_granularity='COLUMN'", columnOptions["Title"]);
    }

    [ConditionalFact]
    public void FluentApiExtensions_configure_drop_if_exists()
    {
        var modelBuilder = new ModelBuilder();
        
        modelBuilder.Entity<Document>(entity =>
        {
            entity.HasIndex(e => e.Title)
                .HasBigQuerySearchIndex()
                .HasBigQueryDropIfExists();
        });

        var model = modelBuilder.FinalizeModel();
        var index = model.FindEntityType(typeof(Document))!.GetIndexes().First();

        Assert.True(index[BigQueryAnnotationNames.IfExists] as bool?);
    }

    [ConditionalFact]
    public void FluentApiExtensions_chaining_works_correctly()
    {
        var modelBuilder = new ModelBuilder();
        
        modelBuilder.Entity<Document>(entity =>
        {
            var indexBuilder = entity.HasIndex(e => e.Title);
            
            indexBuilder.HasBigQuerySearchIndex(ifNotExists: true)
                       .HasBigQuerySearchIndexOptions("analyzer='NO_OP_ANALYZER'")
                       .HasBigQuerySearchIndexColumnOptions("Title", "index_granularity='GLOBAL'");
        });

        var model = modelBuilder.FinalizeModel();
        var index = model.FindEntityType(typeof(Document))!.GetIndexes().First();

        Assert.True(index[BigQueryAnnotationNames.SearchIndex] as bool?);
        Assert.True(index[BigQueryAnnotationNames.IfNotExists] as bool?);
        Assert.Equal("analyzer='NO_OP_ANALYZER'", index[BigQueryAnnotationNames.IndexOptions] as string);
        
        var columnOptions = index[BigQueryAnnotationNames.IndexColumnOptions] as IDictionary<string, string>;
        Assert.NotNull(columnOptions);
        Assert.Equal("index_granularity='GLOBAL'", columnOptions["Title"]);
    }

    private class Document
    {
        public int Id { get; set; }
        public string Title { get; set; } = string.Empty;
        public string Content { get; set; } = string.Empty;
    }

    [ConditionalFact]
    public void FluentApiExtensions_configure_create_or_replace_table()
    {
        var modelBuilder = new ModelBuilder();
        
        modelBuilder.Entity<Document>(entity =>
        {
            entity.HasBigQueryCreateOrReplace();
        });

        var model = modelBuilder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(Document))!;

        Assert.True(entityType[BigQueryAnnotationNames.CreateOrReplace] as bool?);
    }

    [ConditionalFact]
    public void FluentApiExtensions_configure_if_not_exists_table()
    {
        var modelBuilder = new ModelBuilder();
        
        modelBuilder.Entity<Document>(entity =>
        {
            entity.HasBigQueryIfNotExists();
        });

        var model = modelBuilder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(Document))!;

        Assert.True(entityType[BigQueryAnnotationNames.IfNotExists] as bool?);
    }

    [ConditionalFact]
    public void FluentApiExtensions_configure_temp_table()
    {
        var modelBuilder = new ModelBuilder();
        
        modelBuilder.Entity<Document>(entity =>
        {
            entity.HasBigQueryTempTable();
        });

        var model = modelBuilder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(Document))!;

        Assert.True(entityType[BigQueryAnnotationNames.TempTable] as bool?);
    }

    [ConditionalFact]
    public void FluentApiExtensions_configure_temp_table_if_not_exists()
    {
        var modelBuilder = new ModelBuilder();
        
        modelBuilder.Entity<Document>(entity =>
        {
            entity.HasBigQueryTempTableIfNotExists();
        });

        var model = modelBuilder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(Document))!;

        Assert.True(entityType[BigQueryAnnotationNames.TempTable] as bool?);
        Assert.True(entityType[BigQueryAnnotationNames.IfNotExists] as bool?);
    }

    [ConditionalFact]
    public void FluentApiExtensions_configure_create_or_replace_temp_table()
    {
        var modelBuilder = new ModelBuilder();
        
        modelBuilder.Entity<Document>(entity =>
        {
            entity.HasBigQueryCreateOrReplaceTempTable();
        });

        var model = modelBuilder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(Document))!;

        Assert.True(entityType[BigQueryAnnotationNames.CreateOrReplace] as bool?);
        Assert.True(entityType[BigQueryAnnotationNames.TempTable] as bool?);
    }

    [ConditionalFact]
    public void FluentApiExtensions_chaining_works_for_entity_builder()
    {
        var modelBuilder = new ModelBuilder();
        
        modelBuilder.Entity<Document>(entity =>
        {
            entity.HasBigQueryCreateOrReplace()
                 .HasBigQueryTempTable();
        });

        var model = modelBuilder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(Document))!;

        Assert.True(entityType[BigQueryAnnotationNames.CreateOrReplace] as bool?);
        Assert.True(entityType[BigQueryAnnotationNames.TempTable] as bool?);
    }
}