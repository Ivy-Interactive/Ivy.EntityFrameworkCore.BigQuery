using Ivy.EntityFrameworkCore.BigQuery.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit;

namespace Ivy.EntityFrameworkCore.BigQuery.Tests.Update;

public class BigQueryUpdateTest : IClassFixture<BigQueryUpdateTest.BigQueryUpdateTestFixture>
{
    private readonly BigQueryUpdateTestFixture _fixture;
    private readonly TestSqlLoggerFactory _testSqlLoggerFactory
        = new();

    public BigQueryUpdateTest(BigQueryUpdateTestFixture fixture)
    {
        _fixture = fixture;
        _testSqlLoggerFactory = _fixture.TestSqlLoggerFactory;
        _testSqlLoggerFactory.Clear();
    }

    public class BigQueryUpdateTestFixture : SharedStoreFixtureBase<TestContext>, ITestSqlLoggerFactory
    {
        protected override string StoreName => BigQueryUpdateTestStoreFactory.Name;

        protected override ITestStoreFactory TestStoreFactory => BigQueryUpdateTestStoreFactory.Instance;

        public TestSqlLoggerFactory TestSqlLoggerFactory
            => (TestSqlLoggerFactory)ListLoggerFactory;

        protected override void OnModelCreating(ModelBuilder modelBuilder, DbContext context)
        {
            modelBuilder.Entity<Product>(entity =>
            {
                entity.HasKey(e => e.Id);
                entity.ToTable("Products");
            });

            modelBuilder.Entity<Order>(entity =>
            {
                entity.HasKey(e => e.OrderId);
                entity.ToTable("Orders");
            });

            modelBuilder.Entity<Customer>(entity =>
            {
                entity.HasKey(e => e.Id);
                entity.ToTable("Customers");
            });
        }

        public override DbContextOptionsBuilder AddOptions(DbContextOptionsBuilder builder)
            => base.AddOptions(builder).EnableSensitiveDataLogging();

        public void Seed(TestContext context)
        {
            context.Database.EnsureCreated();
            var assembly = typeof(BigQueryUpdateTest).Assembly;
            using var stream = assembly.GetManifestResourceStream("Ivy.EFCore.BigQuery.Tests.BigQueryUpdateTestSeed.sql");
            using var reader = new StreamReader(stream);
            var sql = reader.ReadToEnd();
            context.Database.ExecuteSqlRaw(sql);
        }


    }

    #region Model Classes

    public class Product
    {
        public int Id { get; set; }
        public string Name { get; set; } = null!;
        public decimal Price { get; set; }
        public string? Category { get; set; }
        public DateTime CreatedDate { get; set; }
        public bool IsActive { get; set; }
        public int? SupplierId { get; set; }
        public string? Description { get; set; }
    }

    public class Order
    {
        public int OrderId { get; set; }
        public int CustomerId { get; set; }
        public DateTime OrderDate { get; set; }
        public decimal? TotalAmount { get; set; }
        public string Status { get; set; } = "Pending";
        public string? ShippingAddress { get; set; }
    }

    public class Customer
    {
        public int Id { get; set; }
        public string Name { get; set; } = null!;
        public string? Email { get; set; }
        public DateTime? LastOrderDate { get; set; }
        public bool IsVip { get; set; }
        public string? Region { get; set; }
    }

    public class TestContext : PoolableDbContext
    {
        public TestContext(DbContextOptions options) : base(options) { }

        public DbSet<Product> Products => Set<Product>();
        public DbSet<Order> Orders => Set<Order>();
        public DbSet<Customer> Customers => Set<Customer>();
    }

    #endregion

    #region Basic UPDATE Tests

    [Fact]
    public void ExecuteUpdate_simple_property_update()
    {
        using var context = _fixture.CreateContext();
        _fixture.Seed(context);

        var rowsAffected = context.Products.ExecuteUpdate(s => s.SetProperty(p => p.Price, 29.99m));

        Assert.Equal(5, rowsAffected);
        Assert.True(context.Products.All(p => p.Price == 29.99m));
    }

    [Fact]
    public void ExecuteUpdate_multiple_properties()
    {
        using var context = _fixture.CreateContext();
        _fixture.Seed(context);

        var rowsAffected = context.Products
            .ExecuteUpdate(s => s
                .SetProperty(p => p.Price, 35.99m)
                .SetProperty(p => p.Category, "Electronics"));

        Assert.Equal(5, rowsAffected);
        Assert.True(context.Products.All(p => p.Price == 35.99m && p.Category == "Electronics"));
    }

    [Fact]
    public void ExecuteUpdate_with_where_condition()
    {
        using var context = _fixture.CreateContext();
        _fixture.Seed(context);

        var rowsAffected = context.Products
            .Where(p => p.Category == "Books")
            .ExecuteUpdate(s => s.SetProperty(p => p.IsActive, false));

        Assert.Equal(1, rowsAffected);
        Assert.False(context.Products.Single(p => p.Category == "Books").IsActive);
    }

    [Fact]
    public void ExecuteUpdate_with_complex_where_condition()
    {
        using var context = _fixture.CreateContext();
        _fixture.Seed(context);

        var originalPrice = context.Products.First(p => p.Price > 100 && p.Category == "Electronics" && p.IsActive).Price;
        var rowsAffected = context.Products
            .Where(p => p.Price > 100 && p.Category == "Electronics" && p.IsActive)
            .ExecuteUpdate(s => s.SetProperty(p => p.Price, p => p.Price * 0.9m));

        Assert.Equal(1, rowsAffected);
        var newPrice = context.Products.First(p => p.Id == 1).Price;
        Assert.Equal(originalPrice * 0.9m, newPrice);
    }

    #endregion

    #region Expression-based Updates

    [Fact]
    public void ExecuteUpdate_using_existing_column_values()
    {
        using var context = _fixture.CreateContext();
        _fixture.Seed(context);

        var originalPrices = context.Products.ToDictionary(p => p.Id, p => p.Price);
        var rowsAffected = context.Products
            .ExecuteUpdate(s => s.SetProperty(p => p.Price, p => p.Price * 1.1m));

        Assert.Equal(5, rowsAffected);
        var newPrices = context.Products.ToDictionary(p => p.Id, p => p.Price);
        foreach (var (id, originalPrice) in originalPrices)
        {
            Assert.Equal(originalPrice * 1.1m, newPrices[id]);
        }
    }

    [Fact]
    public void ExecuteUpdate_with_string_concatenation()
    {
        using var context = _fixture.CreateContext();
        _fixture.Seed(context);

        var rowsAffected = context.Products
            .ExecuteUpdate(s => s.SetProperty(p => p.Name, p => "Updated: " + p.Name));

        Assert.Equal(5, rowsAffected);
        Assert.True(context.Products.All(p => p.Name.StartsWith("Updated: ")));
    }

    [Fact]
    public void ExecuteUpdate_with_conditional_expression()
    {
        using var context = _fixture.CreateContext();
        _fixture.Seed(context);

        var rowsAffected = context.Products
            .ExecuteUpdate(s => s.SetProperty(p => p.Category, 
                p => p.Price > 50 ? "Premium" : "Standard"));

        Assert.Equal(5, rowsAffected);
        Assert.Equal("Premium", context.Products.Single(p => p.Id == 1).Category);
        Assert.Equal("Standard", context.Products.Single(p => p.Id == 2).Category);
    }

    [Fact]
    public void ExecuteUpdate_with_datetime_functions()
    {
        using var context = _fixture.CreateContext();
        _fixture.Seed(context);

        var newOrderDate = DateTime.UtcNow;
        var rowsAffected = context.Orders
            .ExecuteUpdate(s => s.SetProperty(o => o.OrderDate, newOrderDate));

        Assert.Equal(3, rowsAffected);
        Assert.True(context.Orders.All(o => o.OrderDate == newOrderDate));
    }

    #endregion

    #region NULL handling

    [Fact]
    public void ExecuteUpdate_set_null_value()
    {
        using var context = _fixture.CreateContext();
        _fixture.Seed(context);

        var rowsAffected = context.Products
            .ExecuteUpdate(s => s.SetProperty(p => p.Category, (string?)null));

        Assert.Equal(5, rowsAffected);
        Assert.True(context.Products.All(p => p.Category == null));
    }

    [Fact]
    public void ExecuteUpdate_with_null_conditional()
    {
        using var context = _fixture.CreateContext();
        _fixture.Seed(context);

        var rowsAffected = context.Products
            .Where(p => p.SupplierId == null)
            .ExecuteUpdate(s => s.SetProperty(p => p.IsActive, false));

        Assert.Equal(0, rowsAffected);
    }

    #endregion

    #region Complex WHERE clauses

    [Fact]
    public void ExecuteUpdate_with_in_clause()
    {
        using var context = _fixture.CreateContext();
        _fixture.Seed(context);

        var categories = new[] { "Books", "Electronics", "Clothing" };
        var rowsAffected = context.Products
            .Where(p => categories.Contains(p.Category!))
            .ExecuteUpdate(s => s.SetProperty(p => p.IsActive, true));

        Assert.Equal(5, rowsAffected);
        Assert.True(context.Products.All(p => p.IsActive));
    }

    [Fact]
    public void ExecuteUpdate_with_like_pattern()
    {
        using var context = _fixture.CreateContext();
        _fixture.Seed(context);

        var rowsAffected = context.Products
            .Where(p => EF.Functions.Like(p.Name, "Lap%"))
            .ExecuteUpdate(s => s.SetProperty(p => p.Category, "Technology"));

        Assert.Equal(1, rowsAffected);
        Assert.Equal("Technology", context.Products.Single(p => p.Name == "Laptop").Category);
    }

    [Fact]
    public void ExecuteUpdate_with_date_range()
    {
        using var context = _fixture.CreateContext();
        _fixture.Seed(context);

        var startDate = new DateTime(2023, 1, 1);
        var endDate = new DateTime(2023, 1, 31);
        
        var rowsAffected = context.Products
            .Where(p => p.CreatedDate >= startDate && p.CreatedDate <= endDate)
            .ExecuteUpdate(s => s.SetProperty(p => p.Description, "Legacy Product"));

        Assert.Equal(2, rowsAffected);
        Assert.True(context.Products.Where(p => p.CreatedDate >= startDate && p.CreatedDate <= endDate).All(p => p.Description == "Legacy Product"));
    }

    #endregion

    #region JOIN scenarios (if supported)

    [Fact]
    public void ExecuteUpdate_with_subquery_condition()
    {
        using var context = _fixture.CreateContext();
        _fixture.Seed(context);

        var rowsAffected = context.Customers
            .Where(c => context.Orders.Any(o => o.CustomerId == c.Id && o.TotalAmount > 1000))
            .ExecuteUpdate(s => s.SetProperty(c => c.IsVip, true));

        Assert.Equal(1, rowsAffected);
        Assert.True(context.Customers.Single(c => c.Id == 1).IsVip);
    }

    #endregion

    #region Edge cases

    [Fact]
    public void ExecuteUpdate_with_computed_value_from_constant()
    {
        using var context = _fixture.CreateContext();
        _fixture.Seed(context);

        var discountRate = 0.15m;
        var originalPrices = context.Products.ToDictionary(p => p.Id, p => p.Price);
        var rowsAffected = context.Products
            .ExecuteUpdate(s => s.SetProperty(p => p.Price, p => p.Price * (1 - discountRate)));

        Assert.Equal(5, rowsAffected);
        var newPrices = context.Products.ToDictionary(p => p.Id, p => p.Price);
        foreach (var (id, originalPrice) in originalPrices)
        {
            Assert.Equal(originalPrice * (1 - discountRate), newPrices[id]);
        }
    }

    [Fact]
    public void ExecuteUpdate_boolean_toggle()
    {
        using var context = _fixture.CreateContext();
        _fixture.Seed(context);

        var originalValues = context.Products.ToDictionary(p => p.Id, p => p.IsActive);
        var rowsAffected = context.Products
            .ExecuteUpdate(s => s.SetProperty(p => p.IsActive, p => !p.IsActive));

        Assert.Equal(5, rowsAffected);
        var newValues = context.Products.ToDictionary(p => p.Id, p => p.IsActive);
        foreach (var (id, originalValue) in originalValues)
        {
            Assert.Equal(!originalValue, newValues[id]);
        }
    }

    [Fact]
    public void ExecuteUpdate_multiple_conditions_with_or()
    {
        using var context = _fixture.CreateContext();
        _fixture.Seed(context);

        var rowsAffected = context.Products
            .Where(p => p.Price < 20 || p.Category == "Books")
            .ExecuteUpdate(s => s.SetProperty(p => p.IsActive, false));

        Assert.Equal(2, rowsAffected);
        Assert.False(context.Products.Single(p => p.Id == 2).IsActive);
        Assert.False(context.Products.Single(p => p.Id == 5).IsActive);
    }

    #endregion

    #region BigQuery-specific features

    [Fact]
    public void ExecuteUpdate_with_array_contains()
    {
        using var context = _fixture.CreateContext();
        _fixture.Seed(context);

        var searchTerms = new[] { "Laptop", "Keyboard" };
        var rowsAffected = context.Products
            .Where(p => searchTerms.Contains(p.Name))
            .ExecuteUpdate(s => s.SetProperty(p => p.Category, "Technology"));

        Assert.Equal(2, rowsAffected);
        Assert.Equal("Technology", context.Products.Single(p => p.Name == "Laptop").Category);
        Assert.Equal("Technology", context.Products.Single(p => p.Name == "Keyboard").Category);
    }

    [Fact]
    public void ExecuteUpdate_with_struct_access()
    {
        using var context = _fixture.CreateContext();
        _fixture.Seed(context);

        var rowsAffected = context.Orders
            .Where(o => o.Status == "Shipped")
            .ExecuteUpdate(s => s.SetProperty(o => o.Status, "Delivered"));

        Assert.Equal(1, rowsAffected);
        Assert.Equal("Delivered", context.Orders.Single(o => o.OrderId == 1).Status);
    }

    #endregion

    #region JOIN scenarios
    [Fact]
    public void ExecuteUpdate_with_join_and_where()
    {
        using var context = _fixture.CreateContext();
        _fixture.Seed(context);

        var rowsAffected = context.Customers
            .Where(c => context.Orders.Any(o => o.CustomerId == c.Id && o.TotalAmount > 200))
            .ExecuteUpdate(s => s.SetProperty(c => c.IsVip, true));

        Assert.Equal(1, rowsAffected);
        Assert.True(context.Customers.Single(c => c.Id == 2).IsVip);
    }

    [Fact]
    public void ExecuteUpdate_with_join_to_update_from_another_table()
    {
        using var context = _fixture.CreateContext();
        _fixture.Seed(context);

        var rowsAffected = context.Orders
            .Where(o => context.Customers.Any(c => c.Id == o.CustomerId && c.Region == "USA"))
            .ExecuteUpdate(s => s.SetProperty(o => o.Status, "Priority"));

        Assert.Equal(2, rowsAffected);
        Assert.True(context.Orders.Where(o => o.CustomerId == 1 || o.CustomerId == 2).All(o => o.Status == "Priority"));
    }

    [Fact]
    public void ExecuteUpdate_update_all_rows()
    {
        using var context = _fixture.CreateContext();
        _fixture.Seed(context);

        var rowsAffected = context.Products
            .ExecuteUpdate(s => s.SetProperty(p => p.Description, "New Description"));

        Assert.Equal(5, rowsAffected);
        Assert.True(context.Products.All(p => p.Description == "New Description"));
    }
    #endregion


}
