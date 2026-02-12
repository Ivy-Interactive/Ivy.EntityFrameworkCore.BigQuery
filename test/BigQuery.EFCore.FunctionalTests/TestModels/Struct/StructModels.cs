using Ivy.EntityFrameworkCore.BigQuery.Metadata;

namespace Ivy.EntityFrameworkCore.BigQuery.TestModels.Struct;

/// <summary>
/// Simple STRUCT type for address information.
/// </summary>
[BigQueryStruct]
public class Address
{
    public string Street { get; set; } = null!;
    public string City { get; set; } = null!;
    public string? ZipCode { get; set; }
}

/// <summary>
/// STRUCT with nested STRUCT.
/// </summary>
[BigQueryStruct]
public class ContactInfo
{
    public string Email { get; set; } = null!;
    public string? Phone { get; set; }
    public Address? MailingAddress { get; set; }
}

/// <summary>
/// STRUCT containing an array of primitives.
/// </summary>
[BigQueryStruct]
public class TagContainer
{
    public string Category { get; set; } = null!;
    public List<string> Tags { get; set; } = null!;
}

/// <summary>
/// Simple STRUCT for use in arrays.
/// </summary>
[BigQueryStruct]
public class OrderItem
{
    public string ProductName { get; set; } = null!;
    public int Quantity { get; set; }
    public decimal UnitPrice { get; set; }
}

/// <summary>
/// Entity with a single STRUCT property.
/// </summary>
public class PersonEntity
{
    public int Id { get; set; }
    public string Name { get; set; } = null!;
    public Address? HomeAddress { get; set; }
}

/// <summary>
/// Entity with nested STRUCTs.
/// </summary>
public class CustomerEntity
{
    public int Id { get; set; }
    public string CustomerName { get; set; } = null!;
    public ContactInfo? Contact { get; set; }
}

/// <summary>
/// Entity with an array of STRUCTs.
/// </summary>
public class OrderEntity
{
    public int Id { get; set; }
    public string OrderNumber { get; set; } = null!;
    public DateTime OrderDate { get; set; }
    public List<OrderItem> Items { get; set; } = null!;
}

/// <summary>
/// Entity with multiple STRUCT properties and arrays.
/// </summary>
public class CompanyEntity
{
    public int Id { get; set; }
    public string CompanyName { get; set; } = null!;
    public Address? HeadquartersAddress { get; set; }
    public List<Address> OfficeLocations { get; set; } = null!;
    public List<string> Industries { get; set; } = null!;
}

/// <summary>
/// Entity with STRUCT containing arrays.
/// </summary>
public class ArticleEntity
{
    public int Id { get; set; }
    public string Title { get; set; } = null!;
    public TagContainer? Metadata { get; set; }
}
