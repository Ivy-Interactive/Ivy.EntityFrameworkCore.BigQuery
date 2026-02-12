using Microsoft.EntityFrameworkCore;

namespace Ivy.EntityFrameworkCore.BigQuery.TestModels.Struct;

public class BigQueryStructContext : DbContext
{
    public BigQueryStructContext(DbContextOptions<BigQueryStructContext> options)
        : base(options)
    {
    }

    public DbSet<PersonEntity> People { get; set; } = null!;
    public DbSet<CustomerEntity> Customers { get; set; } = null!;
    public DbSet<OrderEntity> Orders { get; set; } = null!;

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<PersonEntity>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Name).IsRequired();
        });

        modelBuilder.Entity<CustomerEntity>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.Property(e => e.CustomerName).IsRequired();
        });

        modelBuilder.Entity<OrderEntity>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.Property(e => e.OrderNumber).IsRequired();
            entity.Property(e => e.Items).IsRequired();
        });
    }

    public static async Task SeedAsync(BigQueryStructContext context)
    {
        // PersonEntity - simple STRUCT
        context.People.AddRange(
            new PersonEntity
            {
                Id = 1,
                Name = "Alice Smith",
                HomeAddress = new Address { Street = "123 Main St", City = "Seattle", ZipCode = "98101" }
            },
            new PersonEntity
            {
                Id = 2,
                Name = "Bob Johnson",
                HomeAddress = new Address { Street = "456 Oak Ave", City = "Portland", ZipCode = "97201" }
            },
            new PersonEntity
            {
                Id = 3,
                Name = "Carol Williams",
                HomeAddress = null // nullable STRUCT
            });

        // CustomerEntity - nested STRUCT
        context.Customers.AddRange(
            new CustomerEntity
            {
                Id = 1,
                CustomerName = "Acme Corp",
                Contact = new ContactInfo
                {
                    Email = "contact@acme.com",
                    Phone = "555-1234",
                    MailingAddress = new Address { Street = "789 Business Blvd", City = "San Francisco", ZipCode = "94102" }
                }
            },
            new CustomerEntity
            {
                Id = 2,
                CustomerName = "TechStart Inc",
                Contact = new ContactInfo
                {
                    Email = "hello@techstart.io",
                    Phone = null,
                    MailingAddress = null
                }
            });

        // OrderEntity - ARRAY<STRUCT>
        context.Orders.AddRange(
            new OrderEntity
            {
                Id = 1,
                OrderNumber = "ORD-001",
                OrderDate = new DateTime(2024, 1, 15),
                Items =
                [
                    new OrderItem { ProductName = "Widget", Quantity = 5, UnitPrice = 10.99m },
                    new OrderItem { ProductName = "Gadget", Quantity = 2, UnitPrice = 25.50m }
                ]
            },
            new OrderEntity
            {
                Id = 2,
                OrderNumber = "ORD-002",
                OrderDate = new DateTime(2024, 1, 16),
                Items =
                [
                    new OrderItem { ProductName = "Gizmo", Quantity = 1, UnitPrice = 99.99m }
                ]
            });

        await context.SaveChangesAsync();
    }
}