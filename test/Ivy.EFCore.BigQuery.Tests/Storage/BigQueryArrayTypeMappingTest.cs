using Ivy.EntityFrameworkCore.BigQuery.Storage.Internal;
using Ivy.EntityFrameworkCore.BigQuery.Storage.Internal.Mapping;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.Json;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using Xunit;

#pragma warning disable EF1001 // Internal EF Core API usage.

namespace Ivy.EntityFrameworkCore.BigQuery.Tests.Storage;

public class BigQueryArrayTypeMappingTest
{
    private readonly IRelationalTypeMappingSource _typeMappingSource;

    public BigQueryArrayTypeMappingTest()
    {
        _typeMappingSource = new BigQueryTypeMappingSource(
            new TypeMappingSourceDependencies(
                new ValueConverterSelector(new ValueConverterSelectorDependencies()),
                new JsonValueReaderWriterSource(new JsonValueReaderWriterSourceDependencies()),
                []),
            new RelationalTypeMappingSourceDependencies([]));
    }

    [Fact]
    public void FindMapping_IntArray_ReturnsArrayMapping()
    {
        var mapping = _typeMappingSource.FindMapping(typeof(int[]));

        Assert.NotNull(mapping);
        Assert.Equal("ARRAY<INT64>", mapping.StoreType);
        Assert.Equal(typeof(int[]), mapping.ClrType);
        Assert.IsAssignableFrom<BigQueryArrayTypeMapping>(mapping);
    }

    [Fact]
    public void FindMapping_StringList_ReturnsArrayMapping()
    {
        var mapping = _typeMappingSource.FindMapping(typeof(List<string>));

        Assert.NotNull(mapping);
        Assert.Equal("ARRAY<STRING>", mapping.StoreType);
        Assert.Equal(typeof(List<string>), mapping.ClrType);
        Assert.IsAssignableFrom<BigQueryArrayTypeMapping>(mapping);
    }

    [Fact]
    public void FindMapping_LongArray_ReturnsArrayMapping()
    {
        var mapping = _typeMappingSource.FindMapping(typeof(long[]));

        Assert.NotNull(mapping);
        Assert.Equal("ARRAY<INT64>", mapping.StoreType);
        Assert.Equal(typeof(long[]), mapping.ClrType);
    }

    [Fact]
    public void FindMapping_DoubleList_ReturnsArrayMapping()
    {
        var mapping = _typeMappingSource.FindMapping(typeof(List<double>));

        Assert.NotNull(mapping);
        Assert.Equal("ARRAY<FLOAT64>", mapping.StoreType);
        Assert.Equal(typeof(List<double>), mapping.ClrType);
    }

    [Fact]
    public void FindMapping_BoolArray_ReturnsArrayMapping()
    {
        var mapping = _typeMappingSource.FindMapping(typeof(bool[]));

        Assert.NotNull(mapping);
        Assert.Equal("ARRAY<BOOL>", mapping.StoreType);
        Assert.Equal(typeof(bool[]), mapping.ClrType);
    }

    [Fact]
    public void FindMapping_DateTimeArray_ReturnsArrayMapping()
    {
        var mapping = _typeMappingSource.FindMapping(typeof(DateTime[]));

        Assert.NotNull(mapping);
        Assert.Equal("ARRAY<DATETIME>", mapping.StoreType);
        Assert.Equal(typeof(DateTime[]), mapping.ClrType);
    }

    [Fact]
    public void FindMapping_IListString_ReturnsArrayMapping()
    {
        var mapping = _typeMappingSource.FindMapping(typeof(IList<string>));

        Assert.NotNull(mapping);
        Assert.Equal("ARRAY<STRING>", mapping.StoreType);
        Assert.Equal(typeof(IList<string>), mapping.ClrType);
    }

    [Fact]
    public void FindMapping_ByteArray_DoesNotReturnArrayMapping()
    {
        // byte[] should map to BYTES, not ARRAY<INT64>
        var mapping = _typeMappingSource.FindMapping(typeof(byte[]));

        Assert.NotNull(mapping);
        Assert.Equal("BYTES", mapping.StoreType);
        Assert.IsNotType<BigQueryArrayTypeMapping>(mapping, exactMatch: false);
    }

    [Fact]
    public void FindMapping_StoreType_ReturnsArrayMapping()
    {
        var mapping = _typeMappingSource.FindMapping("ARRAY<INT64>");

        Assert.NotNull(mapping);
        Assert.Equal("ARRAY<INT64>", mapping.StoreType);
    }

    [Fact]
    public void FindMapping_ComplexStoreType_ReturnsArrayMapping()
    {
        var mapping = _typeMappingSource.FindMapping("ARRAY<STRING>");

        Assert.NotNull(mapping);
        Assert.Equal("ARRAY<STRING>", mapping.StoreType);
    }

    [Fact]
    public void GenerateSqlLiteral_IntArray_GeneratesCorrectLiteral()
    {
        var mapping = _typeMappingSource.FindMapping(typeof(int[]));
        Assert.NotNull(mapping);

        var literal = mapping.GenerateSqlLiteral(new[] { 1, 2, 3 });

        Assert.Equal("ARRAY<INT64>[1, 2, 3]", literal);
    }

    [Fact]
    public void GenerateSqlLiteral_StringArray_GeneratesCorrectLiteral()
    {
        var mapping = _typeMappingSource.FindMapping(typeof(string[]));
        Assert.NotNull(mapping);

        var literal = mapping.GenerateSqlLiteral(new[] { "hello", "world" });

        Assert.Equal("ARRAY<STRING>['hello', 'world']", literal);
    }

    [Fact]
    public void GenerateSqlLiteral_EmptyArray_GeneratesCorrectLiteral()
    {
        var mapping = _typeMappingSource.FindMapping(typeof(int[]));
        Assert.NotNull(mapping);

        var literal = mapping.GenerateSqlLiteral(Array.Empty<int>());

        Assert.Equal("ARRAY<INT64>[]", literal);
    }

    [Fact]
    public void GenerateSqlLiteral_BoolArray_GeneratesCorrectLiteral()
    {
        var mapping = _typeMappingSource.FindMapping(typeof(bool[]));
        Assert.NotNull(mapping);

        var literal = mapping.GenerateSqlLiteral(new[] { true, false, true });

        Assert.Equal("ARRAY<BOOL>[TRUE, FALSE, TRUE]", literal);
    }

    [Fact]
    public void GenerateSqlLiteral_DoubleArray_GeneratesCorrectLiteral()
    {
        var mapping = _typeMappingSource.FindMapping(typeof(double[]));
        Assert.NotNull(mapping);

        var literal = mapping.GenerateSqlLiteral(new[] { 1.0, 2.5 });

        Assert.Equal("ARRAY<FLOAT64>[1, 2.5]", literal);
    }

    [Fact]
    public void GenerateSqlLiteral_List_GeneratesCorrectLiteral()
    {
        var mapping = _typeMappingSource.FindMapping(typeof(List<string>));
        Assert.NotNull(mapping);

        var list = new List<string> { "a", "b", "c" };
        var literal = mapping.GenerateSqlLiteral(list);

        Assert.Equal("ARRAY<STRING>['a', 'b', 'c']", literal);
    }

    [Fact]
    public void ArrayMapping_HasElementTypeMapping()
    {
        var mapping = _typeMappingSource.FindMapping(typeof(int[])) as BigQueryArrayTypeMapping;
        Assert.NotNull(mapping);

        var elementMapping = mapping.ElementTypeMapping;
        Assert.NotNull(elementMapping);
        Assert.Equal("INT64", elementMapping.StoreType);
        Assert.Equal(typeof(int), elementMapping.ClrType);
    }

    [Fact]
    public void GenerateSqlLiteral_NullableIntArrayWithNull_GeneratesCorrectLiteral()
    {
        var mapping = _typeMappingSource.FindMapping(typeof(int?[]));
        Assert.NotNull(mapping);

        var literal = mapping.GenerateSqlLiteral(new int?[] { 1, null, 3 });

        Assert.Equal("ARRAY<INT64>[1, NULL, 3]", literal);
    }

    [Fact]
    public void GenerateSqlLiteral_StringArrayWithNull_GeneratesCorrectLiteral()
    {
        var mapping = _typeMappingSource.FindMapping(typeof(string[]));
        Assert.NotNull(mapping);

        var literal = mapping.GenerateSqlLiteral(new string[] { "a", null, "b" });

        Assert.Equal("ARRAY<STRING>['a', NULL, 'b']", literal);
    }
}
