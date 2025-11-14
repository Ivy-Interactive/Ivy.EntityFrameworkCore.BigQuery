# Ivy ADO.NET Provider for BigQuery

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

## Connection String Format

```
AuthMethod=ApplicationDefaultCredentials;ProjectId=your-project-id;DefaultDatasetId=your_dataset
```

**Parameters:**
- `AuthMethod`: `ApplicationDefaultCredentials` or `JsonCredentials` (optional, ApplicationDefaultCredentials is default)
- `ProjectId`: Google Cloud project ID (required)
- `DefaultDatasetId`: Default BigQuery dataset (optional)
- `Timeout`: Connection timeout in seconds (optional, default: 15)
- `CredentialsFile`: Path to the JSON service account key file (Required if AuthMethod=JsonCredentials and JsonCredentials not provided)
- `JsonCredentials`: JSON service account credentials as a string (Required if AuthMethod=JsonCredentials and CredentialsFile not provided)

---

### Accessing BigQueryClient

```csharp
using var connection = new BigQueryConnection(connectionString);
await connection.OpenAsync();

var client = connection.GetBigQueryClient();
```