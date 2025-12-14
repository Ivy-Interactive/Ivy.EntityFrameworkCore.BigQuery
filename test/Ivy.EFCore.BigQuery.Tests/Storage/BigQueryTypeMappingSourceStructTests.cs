using Ivy.EntityFrameworkCore.BigQuery.Metadata;
using Ivy.EntityFrameworkCore.BigQuery.Storage.Internal;
using Ivy.EntityFrameworkCore.BigQuery.Storage.Internal.Mapping;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.Json;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using Microsoft.EntityFrameworkCore.TestModels.ComplexNavigationsModel;
using Xunit;

namespace Ivy.EntityFrameworkCore.BigQuery.Tests.Storage;

#pragma warning disable EF1001 // Internal EF Core API usage.
public class BigQueryTypeMappingSourceStructTests
{
    private readonly BigQueryTypeMappingSource _typeMappingSource;

    public BigQueryTypeMappingSourceStructTests()
    {
        _typeMappingSource = new BigQueryTypeMappingSource(
            new TypeMappingSourceDependencies(
                new ValueConverterSelector(new ValueConverterSelectorDependencies()),
                new JsonValueReaderWriterSource(new JsonValueReaderWriterSourceDependencies()),
                []
            ),
            new RelationalTypeMappingSourceDependencies([]));
    }

    [Fact]
    public void FindMapping_ParsesSimpleStructType()
    {
        var mapping = _typeMappingSource.FindMapping("STRUCT<email STRING, phone STRING>");

        Assert.NotNull(mapping);
        Assert.IsType<BigQueryStructTypeMapping>(mapping);
        Assert.Equal("STRUCT<email STRING, phone STRING>", mapping.StoreType);
    }

    [Fact]
    public void FindMapping_ParsesNestedStructType()
    {
        var mapping = _typeMappingSource.FindMapping(
            "STRUCT<name STRING, address STRUCT<street STRING, city STRING>>");

        Assert.NotNull(mapping);
        Assert.IsType<BigQueryStructTypeMapping>(mapping);
        var structMapping = (BigQueryStructTypeMapping)mapping;
        Assert.Equal(2, structMapping.Fields.Count);
        Assert.Equal("name", structMapping.Fields[0].Name);
        Assert.Equal("address", structMapping.Fields[1].Name);
        Assert.IsType<BigQueryStructTypeMapping>(structMapping.Fields[1].TypeMapping);
    }

    [Fact]
    public void FindMapping_ParsesArrayOfStruct()
    {
        var mapping = _typeMappingSource.FindMapping(
            "ARRAY<STRUCT<name STRING, quantity INT64>>");

        Assert.NotNull(mapping);
        Assert.IsType<BigQueryArrayTypeMapping>(mapping, exactMatch: false);
        var arrayMapping = (BigQueryArrayTypeMapping)mapping;
        Assert.IsType<BigQueryStructTypeMapping>(arrayMapping.ElementTypeMapping);
    }

    [Fact]
    public void FindMapping_HandlesStructWithNumericTypes()
    {
        var mapping = _typeMappingSource.FindMapping(
            "STRUCT<amount NUMERIC, precise BIGNUMERIC>");

        Assert.NotNull(mapping);
        Assert.IsType<BigQueryStructTypeMapping>(mapping);
        var structMapping = (BigQueryStructTypeMapping)mapping;
        Assert.Equal(2, structMapping.Fields.Count);
        Assert.Equal("amount", structMapping.Fields[0].Name);
        Assert.Equal("precise", structMapping.Fields[1].Name);
    }

    [Fact]
    public void FindMapping_HandlesStructWithMultipleNestingLevels()
    {
        var mapping = _typeMappingSource.FindMapping(
            "STRUCT<level1 STRUCT<level2 STRUCT<level3 STRING>>>");

        Assert.NotNull(mapping);
        Assert.IsType<BigQueryStructTypeMapping>(mapping);
        var level1 = (BigQueryStructTypeMapping)mapping;
        Assert.Single(level1.Fields);

        var level2 = (BigQueryStructTypeMapping)level1.Fields[0].TypeMapping;
        Assert.Single(level2.Fields);

        var level3 = (BigQueryStructTypeMapping)level2.Fields[0].TypeMapping;
        Assert.Single(level3.Fields);
        Assert.Equal("level3", level3.Fields[0].Name);
    }

    [Fact]
    public void FindMapping_HandlesStructWithMaxNesting()
    {
        var mapping = _typeMappingSource.FindMapping(
            "STRUCT<level1 STRUCT<level2 STRUCT<level3 STRUCT<level4 STRUCT<level5 STRUCT<level6 STRUCT<level7 STRUCT<level8 STRUCT<level9 STRUCT<level10 STRUCT<level11 STRUCT<level12 STRUCT<level13 STRUCT<level14 STRUCT<level15 STRING>>>>>>>>>>>>>>>"
            );
        Assert.NotNull(mapping);        
        Assert.IsType<BigQueryStructTypeMapping>(mapping);
        var level1 = (BigQueryStructTypeMapping)mapping;
        Assert.Single(level1.Fields);

        var current = level1;
        for (var i = 1; i <= 14; i++)
        {
            Assert.Equal($"level{i}", current.Fields[0].Name);
            Assert.IsType<BigQueryStructTypeMapping>(current.Fields[0].TypeMapping);
            current = (BigQueryStructTypeMapping)current.Fields[0].TypeMapping;
            Assert.Single(current.Fields);
        }

        Assert.Equal("level15", current.Fields[0].Name);
        Assert.IsType<BigQueryStringTypeMapping>(current.Fields[0].TypeMapping);
    }

    [Fact]
    public void FindMapping_DetectsStructFromAttribute()
    {
        var mapping = _typeMappingSource.FindMapping(typeof(AnnotatedStruct));

        Assert.NotNull(mapping);
        Assert.IsType<BigQueryStructTypeMapping>(mapping);
        var structMapping = (BigQueryStructTypeMapping)mapping;
        Assert.Equal(2, structMapping.Fields.Count);
    }

    [Fact]
    public void FindMapping_DoesNotDetectStructForDictionary()
    {
        var mapping = _typeMappingSource.FindMapping(typeof(System.Collections.Generic.Dictionary<string, object>));

        Assert.NotNull(mapping);
        Assert.IsNotType<BigQueryStructTypeMapping>(mapping);
    }

    [BigQueryStruct]
    private class AnnotatedStruct
    {
        public string? Field1 { get; set; }
        public int Field2 { get; set; }
    }
}
