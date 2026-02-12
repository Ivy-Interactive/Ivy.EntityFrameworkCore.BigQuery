using Ivy.EntityFrameworkCore.BigQuery.TestModels.Struct;
using Ivy.EntityFrameworkCore.BigQuery.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace Ivy.EntityFrameworkCore.BigQuery.Query;

public abstract class StructQueryFixture : SharedStoreFixtureBase<BigQueryStructContext>, IQueryFixtureBase, ITestSqlLoggerFactory
{
    protected override ITestStoreFactory TestStoreFactory
        => BigQueryTestStoreFactory.Instance;

    public TestSqlLoggerFactory TestSqlLoggerFactory
        => (TestSqlLoggerFactory)ListLoggerFactory;

    private StructData? _expectedData;

    public override DbContextOptionsBuilder AddOptions(DbContextOptionsBuilder builder)
        => base.AddOptions(builder)
            .ConfigureWarnings(wcb => wcb.Ignore(CoreEventId.CollectionWithoutComparer));

    protected override Task SeedAsync(BigQueryStructContext context)
        => BigQueryStructContext.SeedAsync(context);

    public Func<DbContext> GetContextCreator()
        => CreateContext;

    public ISetSource GetExpectedData()
        => _expectedData ??= new StructData();

    public IReadOnlyDictionary<Type, object> EntitySorters
        => new Dictionary<Type, Func<object, object>>
        {
            { typeof(PersonEntity), e => ((PersonEntity)e)?.Id ?? 0 },
            { typeof(CustomerEntity), e => ((CustomerEntity)e)?.Id ?? 0 },
            { typeof(OrderEntity), e => ((OrderEntity)e)?.Id ?? 0 }
        }.ToDictionary(e => e.Key, e => (object)e.Value);

    public IReadOnlyDictionary<Type, object> EntityAsserters
        => new Dictionary<Type, Action<object?, object?>>
        {
            {
                typeof(PersonEntity), (e, a) =>
                {
                    Assert.Equal(e is null, a is null);
                    if (a is not null)
                    {
                        var expected = (PersonEntity)e!;
                        var actual = (PersonEntity)a;
                        Assert.Equal(expected.Id, actual.Id);
                        Assert.Equal(expected.Name, actual.Name);
                        AssertAddress(expected.HomeAddress, actual.HomeAddress);
                    }
                }
            },
            {
                typeof(CustomerEntity), (e, a) =>
                {
                    Assert.Equal(e is null, a is null);
                    if (a is not null)
                    {
                        var expected = (CustomerEntity)e!;
                        var actual = (CustomerEntity)a;
                        Assert.Equal(expected.Id, actual.Id);
                        Assert.Equal(expected.CustomerName, actual.CustomerName);
                        AssertContactInfo(expected.Contact, actual.Contact);
                    }
                }
            },
            {
                typeof(OrderEntity), (e, a) =>
                {
                    Assert.Equal(e is null, a is null);
                    if (a is not null)
                    {
                        var expected = (OrderEntity)e!;
                        var actual = (OrderEntity)a;
                        Assert.Equal(expected.Id, actual.Id);
                        Assert.Equal(expected.OrderNumber, actual.OrderNumber);
                        Assert.Equal(expected.OrderDate, actual.OrderDate);
                        Assert.Equal(expected.Items.Count, actual.Items.Count);
                    }
                }
            }
        }.ToDictionary(e => e.Key, e => (object)e.Value);

    private static void AssertAddress(Address? expected, Address? actual)
    {
        if (expected is null)
        {
            Assert.Null(actual);
            return;
        }

        Assert.NotNull(actual);
        Assert.Equal(expected.Street, actual.Street);
        Assert.Equal(expected.City, actual.City);
        Assert.Equal(expected.ZipCode, actual.ZipCode);
    }

    private static void AssertContactInfo(ContactInfo? expected, ContactInfo? actual)
    {
        if (expected is null)
        {
            Assert.Null(actual);
            return;
        }

        Assert.NotNull(actual);
        Assert.Equal(expected.Email, actual.Email);
        Assert.Equal(expected.Phone, actual.Phone);
        AssertAddress(expected.MailingAddress, actual.MailingAddress);
    }
}

public class StructData : ISetSource
{
    public IQueryable<TEntity> Set<TEntity>() where TEntity : class
    {
        if (typeof(TEntity) == typeof(PersonEntity))
            return (IQueryable<TEntity>)People.AsQueryable();
        if (typeof(TEntity) == typeof(CustomerEntity))
            return (IQueryable<TEntity>)Customers.AsQueryable();
        if (typeof(TEntity) == typeof(OrderEntity))
            return (IQueryable<TEntity>)Orders.AsQueryable();

        throw new InvalidOperationException($"Unknown entity type: {typeof(TEntity)}");
    }

    public IReadOnlyList<PersonEntity> People { get; } = CreatePeople();
    public IReadOnlyList<CustomerEntity> Customers { get; } = CreateCustomers();
    public IReadOnlyList<OrderEntity> Orders { get; } = CreateOrders();

    private static List<PersonEntity> CreatePeople() =>
    [
        new()
        {
            Id = 1,
            Name = "Alice Smith",
            HomeAddress = new Address { Street = "123 Main St", City = "Seattle", ZipCode = "98101" }
        },
        new()
        {
            Id = 2,
            Name = "Bob Johnson",
            HomeAddress = new Address { Street = "456 Oak Ave", City = "Portland", ZipCode = "97201" }
        },
        new()
        {
            Id = 3,
            Name = "Carol Williams",
            HomeAddress = null
        }
    ];

    private static List<CustomerEntity> CreateCustomers() =>
    [
        new()
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
        new()
        {
            Id = 2,
            CustomerName = "TechStart Inc",
            Contact = new ContactInfo
            {
                Email = "hello@techstart.io",
                Phone = null,
                MailingAddress = null
            }
        }
    ];

    private static List<OrderEntity> CreateOrders() =>
    [
        new()
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
        new()
        {
            Id = 2,
            OrderNumber = "ORD-002",
            OrderDate = new DateTime(2024, 1, 16),
            Items =
            [
                new OrderItem { ProductName = "Gizmo", Quantity = 1, UnitPrice = 99.99m }
            ]
        }
    ];
}
