using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.Json;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using Ivy.EntityFrameworkCore.BigQuery.Storage.Internal;
using Ivy.EntityFrameworkCore.BigQuery.Storage.Internal.Mapping;
using Ivy.EntityFrameworkCore.BigQuery.Metadata;
using Xunit;

namespace Ivy.EntityFrameworkCore.BigQuery.Tests.Storage;

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
        Assert.IsType<BigQueryArrayTypeMapping>(mapping);
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
