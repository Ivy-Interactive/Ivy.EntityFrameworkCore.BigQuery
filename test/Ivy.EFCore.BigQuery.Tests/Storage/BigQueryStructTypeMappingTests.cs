using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.Json;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using Ivy.EntityFrameworkCore.BigQuery.Storage.Internal;
using Ivy.EntityFrameworkCore.BigQuery.Storage.Internal.Mapping;
using Xunit;

namespace Ivy.EntityFrameworkCore.BigQuery.Storage;

public class BigQueryStructTypeMappingTests
{
    private readonly IRelationalTypeMappingSource _typeMappingSource;

    public BigQueryStructTypeMappingTests()
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
    public void BuildStoreType_GeneratesCorrectStructDefinition()
    {
        var fields = new List<BigQueryStructTypeMapping.StructFieldInfo>
        {
            new("email", _typeMappingSource.FindMapping(typeof(string))!, typeof(string)),
            new("phone", _typeMappingSource.FindMapping(typeof(string))!, typeof(string))
        };

        var mapping = new BigQueryStructTypeMapping(typeof(ContactInfo), fields);

        Assert.Equal("STRUCT<email STRING, phone STRING>", mapping.StoreType);
    }

    [Fact]
    public void BuildStoreType_HandlesNestedStructs()
    {
        var innerFields = new List<BigQueryStructTypeMapping.StructFieldInfo>
        {
            new("street", _typeMappingSource.FindMapping(typeof(string))!, typeof(string)),
            new("city", _typeMappingSource.FindMapping(typeof(string))!, typeof(string))
        };
        var innerMapping = new BigQueryStructTypeMapping(typeof(Address), innerFields);

        var outerFields = new List<BigQueryStructTypeMapping.StructFieldInfo>
        {
            new("name", _typeMappingSource.FindMapping(typeof(string))!, typeof(string)),
            new("address", innerMapping, typeof(Address))
        };

        var mapping = new BigQueryStructTypeMapping(typeof(Person), outerFields);

        Assert.Equal("STRUCT<name STRING, address STRUCT<street STRING, city STRING>>", mapping.StoreType);
    }

    [Fact]
    public void GenerateNonNullSqlLiteral_CreatesTypedStructLiteral()
    {
        var fields = new List<BigQueryStructTypeMapping.StructFieldInfo>
        {
            new("email", _typeMappingSource.FindMapping(typeof(string))!, typeof(string)),
            new("phone", _typeMappingSource.FindMapping(typeof(string))!, typeof(string))
        };

        var mapping = new BigQueryStructTypeMapping(typeof(ContactInfo), fields);
        var value = new ContactInfo { Email = "test@example.com", Phone = "555-1234" };

        var literal = mapping.GenerateSqlLiteral(value);

        Assert.Contains("STRUCT<email STRING, phone STRING>", literal);
        Assert.Contains("test@example.com", literal);
        Assert.Contains("555-1234", literal);
    }

    [Fact]
    public void ValueConverter_ConvertsDictionaryToStruct_CaseInsensitive()
    {
        var fields = new List<BigQueryStructTypeMapping.StructFieldInfo>
        {
            new("Email", _typeMappingSource.FindMapping(typeof(string))!, typeof(string)),
            new("Phone", _typeMappingSource.FindMapping(typeof(string))!, typeof(string))
        };

        var mapping = new BigQueryStructTypeMapping(typeof(ContactInfo), fields);
        var converter = mapping.Converter;
        Assert.NotNull(converter);

        // BigQuery returns lowercase field names
        var dict = new Dictionary<string, object>
        {
            ["email"] = "test@example.com",
            ["phone"] = "555-1234"
        };

        var result = (ContactInfo)converter.ConvertFromProvider(dict)!;

        Assert.Equal("test@example.com", result.Email);
        Assert.Equal("555-1234", result.Phone);
    }

    [Fact]
    public void ValueConverter_ConvertsStructToDictionary()
    {
        var fields = new List<BigQueryStructTypeMapping.StructFieldInfo>
        {
            new("Email", _typeMappingSource.FindMapping(typeof(string))!, typeof(string)),
            new("Phone", _typeMappingSource.FindMapping(typeof(string))!, typeof(string))
        };

        var mapping = new BigQueryStructTypeMapping(typeof(ContactInfo), fields);
        var converter = mapping.Converter;
        Assert.NotNull(converter);

        var value = new ContactInfo { Email = "test@example.com", Phone = "555-1234" };
        var result = (IDictionary<string, object>)converter.ConvertToProvider(value)!;

        Assert.Equal("test@example.com", result["Email"]);
        Assert.Equal("555-1234", result["Phone"]);
    }

    [Fact]
    public void ValueConverter_HandlesNullValues()
    {
        var fields = new List<BigQueryStructTypeMapping.StructFieldInfo>
        {
            new("Email", _typeMappingSource.FindMapping(typeof(string))!, typeof(string)),
            new("Phone", _typeMappingSource.FindMapping(typeof(string))!, typeof(string))
        };

        var mapping = new BigQueryStructTypeMapping(typeof(ContactInfo), fields);
        var converter = mapping.Converter;
        Assert.NotNull(converter);

        var result = converter.ConvertFromProvider(null);
        Assert.Null(result);
    }

    [Fact]
    public void ValueComparer_ComparesFieldByField()
    {
        var fields = new List<BigQueryStructTypeMapping.StructFieldInfo>
        {
            new("Email", _typeMappingSource.FindMapping(typeof(string))!, typeof(string)),
            new("Phone", _typeMappingSource.FindMapping(typeof(string))!, typeof(string))
        };

        var mapping = new BigQueryStructTypeMapping(typeof(ContactInfo), fields);
        var comparer = mapping.Comparer;

        var value1 = new ContactInfo { Email = "test@example.com", Phone = "555-1234" };
        var value2 = new ContactInfo { Email = "test@example.com", Phone = "555-1234" };
        var value3 = new ContactInfo { Email = "different@example.com", Phone = "555-1234" };

        Assert.True(comparer.Equals(value1, value2));
        Assert.False(comparer.Equals(value1, value3));
    }

    // Test helper classes
    private class ContactInfo
    {
        public string? Email { get; set; }
        public string? Phone { get; set; }
    }

    private class Address
    {
        public string? Street { get; set; }
        public string? City { get; set; }
    }

    private class Person
    {
        public string? Name { get; set; }
        public Address? Address { get; set; }
    }
}
