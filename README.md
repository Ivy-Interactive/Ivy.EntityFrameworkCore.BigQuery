Created with ❤️ by the [Ivy](https://github.com/Ivy-Interactive/Ivy-Framework) team.

# Ivy.EntityFrameworkCore.BigQuery

Entity Framework Core and ADO.NET providers for Google BigQuery.

---

## ADO.NET Provider

A standard ADO.NET provider (`Ivy.Data.BigQuery`) for BigQuery, implementing `DbConnection`, `DbCommand`, and `DbDataReader`.

### Getting Started

```csharp
using Ivy.Data.BigQuery;

var connectionString = "AuthMethod=ApplicationDefaultCredentials;ProjectId=my-project;DefaultDatasetId=my_dataset";
using var connection = new BigQueryConnection(connectionString);
await connection.OpenAsync();

using var command = connection.CreateCommand();
command.CommandText = "SELECT name, age FROM users WHERE age > @minAge";
command.Parameters.Add(new BigQueryParameter("@minAge", BigQueryDbType.Int64, 18));

using var reader = await command.ExecuteReaderAsync();
while (await reader.ReadAsync())
{
    Console.WriteLine($"{reader.GetString(0)}, {reader.GetInt64(1)}");
}
```

### Accessing BigQueryClient

```csharp
using var connection = new BigQueryConnection(connectionString);
await connection.OpenAsync();

var client = connection.GetBigQueryClient();
```

---

## Entity Framework Core Provider

An Entity Framework Core provider (`Ivy.EntityFrameworkCore.BigQuery`) that supports LINQ queries, migrations, and scaffolding.

### Getting Started


```csharp
using var context = new MyDbContext();
context.Customers.Add(new Customer
{
    Name = "John Doe",
    Email = "john@example.com"
});
await context.SaveChangesAsync();

public class MyDbContext : DbContext
{
    public DbSet<Customer> Customers { get; set; }
    public DbSet<Order> Orders { get; set; }

    protected override void OnConfiguring(DbContextOptionsBuilder options)
    {
        options.UseBigQuery(
            "AuthMethod=ApplicationDefaultCredentials;ProjectId=my-project;DefaultDatasetId=my_dataset"
        );
    }
}

public class Customer
{
    public int CustomerId { get; set; }
    public string Name { get; set; }
    public string Email { get; set; }
}
```


**Migrations:**

```bash
dotnet ef migrations add InitialCreate
dotnet ef database update
```

**Scaffold from existing BigQuery dataset:**

```bash
dotnet ef dbcontext scaffold "AuthMethod=ApplicationDefaultCredentials;ProjectId=my-project;DefaultDatasetId=my_dataset" Ivy.EntityFrameworkCore.BigQuery --output-dir Models
```

### Accessing BigQueryClient


```csharp
using var context = new MyDbContext();
var connection = (BigQueryConnection)context.Database.GetDbConnection();
await connection.OpenAsync();

var client = connection.GetBigQueryClient();
```

---

## Connection String Format

```
AuthMethod=ApplicationDefaultCredentials;ProjectId=your-project-id;DefaultDatasetId=your_dataset
```

**Parameters:**
- `AuthMethod`: `ApplicationDefaultCredentials` (required)
- `ProjectId`: Google Cloud project ID (required)
- `DefaultDatasetId`: Default BigQuery dataset (optional)
- `Timeout`: Connection timeout in seconds (optional, default: 15)
- `CredentialsFile` (Required if AuthMethod=JsonCredentials and JsonCredentials not provided): Path to the JSON service account key file.
- `JsonCredentials` (Required if AuthMethod=JsonCredentials and CredentialsFile not provided): JSON service account credentials as a string.

---

## Tests

Warning: Tests are extremely slow due to the nature of BigQuery. Northwind fixtures take ~1minute to seed.

### ADO.NET provider tests

The fastest way is to run the powershell script (results saved in TestResults folder). 

```powershell .\test\Ivy.EFCore.BigQuery.Data.Conformance.Tests\tests.ps1```

Run the BQ emulator and start tests from within VS:

```docker compose -f ".\test\Ivy.EFCore.BigQuery.Conformance.Tests\docker\docker-compose.yml" up -d```

Run with your own BigQuery project by setting `BQ_ADO_CONN_STRING` environment variable to your connection string. Create a `ado_tests` dataset with `select_value` table in your project.

## EFCore provider tests

Set a `BQ_EFCORE_TEST_CONN_STRING` environment variable to your connection string.

---

## Type Mapping

The provider automatically maps between BigQuery and .NET types. Reading INT64 into smaller types (`int`/`short`/`byte`) throws an overflow exception if the value exceeds the target type's range.

All value types support their nullable equivalents (e.g., `long?`, `bool?`).

| BigQuery Type    | Default CLR Type   | Other Possible CLR Types                   |
|------------------|--------------------|--------------------------------------------|
| BOOL             | bool               |                                            |
| BYTES            | byte[]             |                                            |
| STRING           | string             | Guid                                       |
| INT64            | long               | int, short, byte                           |
| FLOAT64          | double             | float                                      |
| NUMERIC          | [BigQueryNumeric](https://docs.cloud.google.com/dotnet/docs/reference/Google.Cloud.BigQuery.V2/latest/Google.Cloud.BigQuery.V2.BigQueryNumeric)    |                                            |
| BIGNUMERIC       | [BigQueryBigNumeric](https://docs.cloud.google.com/dotnet/docs/reference/Google.Cloud.BigQuery.V2/latest/Google.Cloud.BigQuery.V2.BigQueryBigNumeric) | decimal                                    |
| DATE             | DateOnly           |                                            |
| DATETIME         | DateTime           |                                            |
| TIME             | TimeOnly           |                                            |
| TIMESTAMP        | DateTimeOffset     |                                            |
| JSON             | string             | JsonDocument, JsonElement                  |

BigQuery `ARRAY` types can be mapped to .NET arrays, `List<T>`, or other `IEnumerable<T>` implementations.

| Array Type           | Default CLR Type     | Other Possible CLR Types                         |
|----------------------|----------------------|--------------------------------------------------|
| ARRAY\<BOOL\>        | bool[]               | List\<bool\>, IList\<bool\>                      |
| ARRAY\<BYTES\>       | byte[][]             | List\<byte[]\>                                   |
| ARRAY\<STRING\>      | string[]             | List\<string\>                                   |
| ARRAY\<INT64\>       | long[]               | List\<long\>, int[], List\<int\>                 |
| ARRAY\<FLOAT64\>     | double[]             | List\<double\>, float[], List\<float\>           |
| ARRAY\<NUMERIC\>     | [BigQueryNumeric](https://docs.cloud.google.com/dotnet/docs/reference/Google.Cloud.BigQuery.V2/latest/Google.Cloud.BigQuery.V2.BigQueryNumeric)[]    | List\<BigQueryNumeric\>                          |
| ARRAY\<BIGNUMERIC\>  | [BigQueryBigNumeric](https://docs.cloud.google.com/dotnet/docs/reference/Google.Cloud.BigQuery.V2/latest/Google.Cloud.BigQuery.V2.BigQueryBigNumeric)[] | List\<BigQueryBigNumeric\>                       |
| ARRAY\<DATE\>        | DateOnly[]           | List\<DateOnly\>                                 |
| ARRAY\<DATETIME\>    | DateTime[]           | List\<DateTime\>                                 |
| ARRAY\<TIME\>        | TimeOnly[]           | List\<TimeOnly\>                                 |
| ARRAY\<TIMESTAMP\>   | DateTimeOffset[]     | List\<DateTimeOffset\>                           |

### Special Type Notes

- **TimeSpan**: Mapped to `INT64` (stored as total microseconds)
- **Guid**: Mapped to `STRING` (stored as UUID string representation)
- **Enums**: Mapped to their underlying numeric type (typically `INT64`)
- **STRUCT**: Classes decorated with `[BigQueryStruct]` (or `.HasColumnType("STRUCT<...>")`) are mapped to BigQuery STRUCT types
