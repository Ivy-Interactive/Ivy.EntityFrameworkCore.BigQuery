# Ivy Entity Framework Core provider for BigQuery

Ivy.EntityFrameworkCore.BigQuery is an Entity Framework Core provider that supports LINQ queries, migrations, and scaffolding.

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
- `AuthMethod`: `ApplicationDefaultCredentials` or `JsonCredentials` (required)
- `ProjectId`: Google Cloud project ID (required)
- `DefaultDatasetId`: Default BigQuery dataset (optional)
- `Timeout`: Connection timeout in seconds (optional, default: 15)
- `CredentialsFile`: Path to the JSON service account key file (Required if AuthMethod=JsonCredentials and JsonCredentials not provided)
- `JsonCredentials`: JSON service account credentials as a string (Required if AuthMethod=JsonCredentials and CredentialsFile not provided)

---