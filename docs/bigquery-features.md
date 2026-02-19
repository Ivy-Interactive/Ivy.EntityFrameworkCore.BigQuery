# BigQuery-Specific Features

This document covers BigQuery-specific features supported by the provider that differ from standard relational databases.

## Dataset Location (Region)

BigQuery datasets are stored in a specific geographic location. Once created, a dataset's location **cannot be changed**.

### Specifying Location

Set the `Location` parameter in your connection string:

```csharp
var connectionString = "AuthMethod=ApplicationDefaultCredentials;ProjectId=my-project;DefaultDatasetId=my_dataset;Location=EU";
```

When creating a dataset (via `EnsureCreated()` or migrations), the provider generates:

```sql
CREATE SCHEMA IF NOT EXISTS my_dataset OPTIONS(location='EU')
```

For available locations, see [BigQuery locations](https://cloud.google.com/bigquery/docs/locations).

---

# STRUCT type

The provider supports mapping C# classes to BigQuery's STRUCT type using the `[BigQueryStruct]` attribute.

```csharp
[BigQueryStruct]
public class Address
{
    public string Street { get; set; }
    public string City { get; set; }
}

public class Person
{
    public int Id { get; set; }
    public string Name { get; set; }
    public Address HomeAddress { get; set; }  // STRUCT<STRING, STRING>
    public List<Address> OtherAdresses // ARRAY<STRUCT<STRING, STRING>>
}
```


## See Also

- [Correlated Subqueries](correlated-subqueries.md) - Limitations and workarounds for correlated subqueries in BigQuery
