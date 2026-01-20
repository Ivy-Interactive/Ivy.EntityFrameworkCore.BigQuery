using System.ComponentModel.DataAnnotations.Schema;
using System.Text.Json.Serialization;
using Ivy.EntityFrameworkCore.BigQuery.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit.Abstractions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query;

public class JsonPocoQueryTest : IClassFixture<JsonPocoQueryTest.JsonPocoQueryFixture>
{
    private JsonPocoQueryFixture Fixture { get; }

    public JsonPocoQueryTest(JsonPocoQueryFixture fixture, ITestOutputHelper testOutputHelper)
    {
        Fixture = fixture;
        Fixture.TestSqlLoggerFactory.Clear();
        Fixture.TestSqlLoggerFactory.SetTestOutputHelper(testOutputHelper);
    }

    [Fact]
    public void Roundtrip()
    {
        using var ctx = CreateContext();
        var customer = ctx.JsonEntities.Single(e => e.Id == 1).Customer;
        var types = customer.VariousTypes;

        Assert.Equal("foo", types.String);
        Assert.Equal(8, types.Int32);
        Assert.Equal(8, types.Int64);
    }

    [Fact(Skip = "BigQuery does not support JSON equality - needs TO_JSON_STRING translation")]
    public void Literal()
    {
        using var ctx = CreateContext();

        Assert.Empty(ctx.JsonEntities.Where(e => e.Customer == new Customer { Name = "Test customer", Age = 80 }));

        AssertSql(
            """
            SELECT `j`.`Id`, `j`.`Customer`
            FROM `JsonEntities` AS `j`
            WHERE TO_JSON_STRING(`j`.`Customer`) = TO_JSON_STRING(JSON '{"Name":"Test customer","Age":80,"ID":"00000000-0000-0000-0000-000000000000","is_vip":false,"Statistics":null,"Orders":null,"VariousTypes":null}')
            """);
    }

    [Fact(Skip = "BigQuery does not support JSON equality - needs TO_JSON_STRING translation")]
    public void Parameter()
    {
        using var ctx = CreateContext();

        var expected = ctx.JsonEntities.Find(1).Customer;
        var actual = ctx.JsonEntities.Single(e => e.Customer == expected).Customer;

        Assert.Equal(actual.Name, expected.Name);
    }

    #region Output

    [Fact]
    public void Output_string()
    {
        using var ctx = CreateContext();

        var x = ctx.JsonEntities.Single(e => e.Customer.Name == "Joe");

        Assert.Equal("Joe", x.Customer.Name);

        AssertSql(
            """
            SELECT `j`.`Id`, `j`.`Customer`
            FROM `JsonEntities` AS `j`
            WHERE STRING(`j`.`Customer`.Name) = 'Joe'
            LIMIT 2
            """);
    }

    [Fact]
    public void Output_int()
    {
        using var ctx = CreateContext();

        var x = ctx.JsonEntities.Single(e => e.Customer.Age < 30);

        Assert.Equal("Joe", x.Customer.Name);

        AssertSql(
            """
            SELECT `j`.`Id`, `j`.`Customer`
            FROM `JsonEntities` AS `j`
            WHERE INT64(`j`.`Customer`.Age) < 30
            LIMIT 2
            """);
    }

    [Fact]
    public void Output_Guid()
    {
        using var ctx = CreateContext();

        var x = ctx.JsonEntities.Single(e => e.Customer.ID == Guid.Empty);

        Assert.Equal("Joe", x.Customer.Name);

        AssertSql(
            """
            SELECT `j`.`Id`, `j`.`Customer`
            FROM `JsonEntities` AS `j`
            WHERE STRING(`j`.`Customer`.ID) = '00000000-0000-0000-0000-000000000000'
            LIMIT 2
            """);
    }

    [Fact]
    public void Output_bool()
    {
        using var ctx = CreateContext();

        var x = ctx.JsonEntities.Single(e => !e.Customer.IsVip);

        Assert.Equal("Joe", x.Customer.Name);

        AssertSql(
            """
            SELECT `j`.`Id`, `j`.`Customer`
            FROM `JsonEntities` AS `j`
            WHERE NOT (BOOL(`j`.`Customer`.is_vip))
            LIMIT 2
            """);
    }

    #endregion

    #region Nested

    [Fact]
    public void Nested_property()
    {
        using var ctx = CreateContext();

        var x = ctx.JsonEntities.Single(e => e.Customer.Statistics.Visits == 4);

        Assert.Equal("Joe", x.Customer.Name);

        AssertSql(
            """
            SELECT `j`.`Id`, `j`.`Customer`
            FROM `JsonEntities` AS `j`
            WHERE INT64(`j`.`Customer`.Statistics.Visits) = 4
            LIMIT 2
            """);
    }

    [Fact]
    public void Nested_property_twice()
    {
        using var ctx = CreateContext();

        var x = ctx.JsonEntities.Single(e => e.Customer.Statistics.Nested.SomeProperty == 10);

        Assert.Equal("Joe", x.Customer.Name);

        AssertSql(
            """
            SELECT `j`.`Id`, `j`.`Customer`
            FROM `JsonEntities` AS `j`
            WHERE INT64(`j`.`Customer`.Statistics.Nested.SomeProperty) = 10
            LIMIT 2
            """);
    }

    #endregion

    #region Array

    [Fact(Skip = "https://github.com/dotnet/efcore/issues/30386")]
    public void Array_element_access()
    {
        using var ctx = CreateContext();

        var x = ctx.JsonEntities.Single(e => e.Customer.Orders[0].Price == 99.5);

        Assert.Equal("Joe", x.Customer.Name);

        AssertSql(
            """
            SELECT `j`.`Id`, `j`.`Customer`
            FROM `JsonEntities` AS `j`
            WHERE FLOAT64(`j`.`Customer`.Orders[0].Price) = 99.5
            LIMIT 2
            """);
    }

    [Fact(Skip = "https://github.com/dotnet/efcore/issues/30386")]
    public void Array_nested_property()
    {
        using var ctx = CreateContext();

        var x = ctx.JsonEntities.Single(e => e.Customer.Statistics.Nested.IntArray[1] == 4);

        Assert.Equal("Joe", x.Customer.Name);

        AssertSql(
            """
            SELECT `j`.`Id`, `j`.`Customer`
            FROM `JsonEntities` AS `j`
            WHERE INT64(`j`.`Customer`.Statistics.Nested.IntArray[1]) = 4
            LIMIT 2
            """);
    }

    [Fact(Skip = "https://github.com/dotnet/efcore/issues/30386")]
    public void Array_parameter_index()
    {
        using var ctx = CreateContext();

        var i = 1;
        var x = ctx.JsonEntities.Single(e => e.Customer.Statistics.Nested.IntArray[i] == 4);

        Assert.Equal("Joe", x.Customer.Name);

        AssertSql(
            """
            @__i_0='1'

            SELECT `j`.`Id`, `j`.`Customer`
            FROM `JsonEntities` AS `j`
            WHERE INT64(`j`.`Customer`.Statistics.Nested.IntArray[@__i_0]) = 4
            LIMIT 2
            """);
    }

    #endregion

    #region String operations

    [Fact]
    public void Like()
    {
        using var ctx = CreateContext();

        var x = ctx.JsonEntities.Single(e => e.Customer.Name.StartsWith("J"));

        Assert.Equal("Joe", x.Customer.Name);

        AssertSql(
            """
            SELECT `j`.`Id`, `j`.`Customer`
            FROM `JsonEntities` AS `j`
            WHERE STRING(`j`.`Customer`.Name) LIKE 'J%'
            LIMIT 2
            """);
    }

    [Fact]
    public void Contains()
    {
        using var ctx = CreateContext();

        var x = ctx.JsonEntities.Single(e => e.Customer.Name.Contains("Jo"));

        Assert.Equal("Joe", x.Customer.Name);

        AssertSql(
            """
            SELECT `j`.`Id`, `j`.`Customer`
            FROM `JsonEntities` AS `j`
            WHERE STRING(`j`.`Customer`.Name) LIKE '%Jo%'
            LIMIT 2
            """);
    }

    #endregion

    #region JsonPropertyName

    [Fact]
    public void JsonPropertyName_mapping()
    {
        using var ctx = CreateContext();

        var x = ctx.JsonEntities.Single(e => e.Customer.IsVip);

        Assert.Equal("Moe", x.Customer.Name);

        AssertSql(
            """
            SELECT `j`.`Id`, `j`.`Customer`
            FROM `JsonEntities` AS `j`
            WHERE BOOL(`j`.`Customer`.is_vip)
            LIMIT 2
            """);
    }

    #endregion

    private JsonPocoQueryContext CreateContext() => Fixture.CreateContext();

    private void AssertSql(params string[] expected)
        => Fixture.TestSqlLoggerFactory.AssertBaseline(expected);

    #region Context and Fixture

    public class JsonPocoQueryContext : PoolableDbContext
    {
        public DbSet<JsonEntity> JsonEntities { get; set; }

        public JsonPocoQueryContext(DbContextOptions options) : base(options) { }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);

            // Only JsonEntity is an actual entity - all POCOs are serialized within JSON columns
            modelBuilder.Entity<JsonEntity>(entity =>
            {
                entity.HasKey(e => e.Id);

                // Configure JSON property with custom value comparer (can't use expressions, so do it via action)
                entity.Property(e => e.Customer)
                    .HasColumnType("JSON");
            });

        }

        public static async Task SeedAsync(JsonPocoQueryContext context)
        {
            await context.JsonEntities.AddRangeAsync(
                new JsonEntity
                {
                    Id = 1,
                    Customer = new Customer
                    {
                        Name = "Joe",
                        Age = 25,
                        ID = Guid.Empty,
                        IsVip = false,
                        Statistics = new Statistics
                        {
                            Visits = 4,
                            Purchases = 3,
                            Nested = new NestedStatistics
                            {
                                SomeProperty = 10,
                                IntArray = new[] { 3, 4 }
                            }
                        },
                        Orders = new[]
                        {
                            new Order
                            {
                                Price = 99.5,
                                ShippingAddress = "Some address 1"
                            },
                            new Order
                            {
                                Price = 23,
                                ShippingAddress = "Some address 2"
                            }
                        },
                        VariousTypes = new VariousTypes
                        {
                            String = "foo",
                            Int32 = 8,
                            Int64 = 8,
                            Bool = false
                        }
                    }
                },
                new JsonEntity
                {
                    Id = 2,
                    Customer = new Customer
                    {
                        Name = "Moe",
                        Age = 35,
                        ID = Guid.Parse("3272b593-bfe2-4ecf-81ae-4242b0632465"),
                        IsVip = true,
                        Statistics = new Statistics
                        {
                            Visits = 20,
                            Purchases = 25,
                            Nested = new NestedStatistics
                            {
                                SomeProperty = 20,
                                IntArray = new[] { 5, 6, 7 }
                            }
                        },
                        Orders = new[]
                        {
                            new Order
                            {
                                Price = 5,
                                ShippingAddress = "Moe's address"
                            }
                        },
                        VariousTypes = new VariousTypes
                        {
                            String = "bar",
                            Int32 = 9,
                            Int64 = 9,
                            Bool = true
                        }
                    }
                }
            );

            await context.SaveChangesAsync();
        }
    }

    public class JsonEntity
    {
        public int Id { get; set; }

        [Column(TypeName = "JSON")]
        public Customer Customer { get; set; }
    }

    public class Customer
    {
        public string Name { get; set; }
        public int Age { get; set; }
        public Guid ID { get; set; }

        [JsonPropertyName("is_vip")]
        public bool IsVip { get; set; }

        public Statistics Statistics { get; set; }
        public Order[] Orders { get; set; }
        public VariousTypes VariousTypes { get; set; }
    }

    public class Statistics
    {
        public long Visits { get; set; }
        public int Purchases { get; set; }
        public NestedStatistics Nested { get; set; }
    }

    public class NestedStatistics
    {
        public int SomeProperty { get; set; }
        public int[] IntArray { get; set; }
    }

    public class Order
    {
        public double Price { get; set; }
        public string ShippingAddress { get; set; }
    }

    public class VariousTypes
    {
        public string String { get; set; }
        public int Int32 { get; set; }
        public long Int64 { get; set; }
        public bool Bool { get; set; }
    }

    public class JsonPocoQueryFixture : SharedStoreFixtureBase<JsonPocoQueryContext>
    {
        protected override string StoreName => "JsonPocoQueryTest";

        protected override ITestStoreFactory TestStoreFactory
            => BigQueryTestStoreFactory.Instance;

        public TestSqlLoggerFactory TestSqlLoggerFactory
            => (TestSqlLoggerFactory)ListLoggerFactory;

        protected override Task SeedAsync(JsonPocoQueryContext context)
            => JsonPocoQueryContext.SeedAsync(context);
    }

    #endregion
}
