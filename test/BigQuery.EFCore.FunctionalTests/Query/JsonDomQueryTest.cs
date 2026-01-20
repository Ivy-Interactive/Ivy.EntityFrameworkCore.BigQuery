using System.ComponentModel.DataAnnotations.Schema;
using System.Text.Json;
using Ivy.EntityFrameworkCore.BigQuery.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit.Abstractions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query;

public class JsonDomQueryTest : IClassFixture<JsonDomQueryTest.JsonDomQueryFixture>
{
    private JsonDomQueryFixture Fixture { get; }

    public JsonDomQueryTest(JsonDomQueryFixture fixture, ITestOutputHelper testOutputHelper)
    {
        Fixture = fixture;
        Fixture.TestSqlLoggerFactory.Clear();
        Fixture.TestSqlLoggerFactory.SetTestOutputHelper(testOutputHelper);
    }

    [Fact]
    public void Roundtrip()
    {
        using var ctx = CreateContext();

        var customer = ctx.JsonEntities.Single(e => e.Id == 1).CustomerElement;
        var types = customer.GetProperty("VariousTypes");

        Assert.Equal("foo", types.GetProperty("String").GetString());
        Assert.Equal(8, types.GetProperty("Int32").GetInt32());
        Assert.Equal(8, types.GetProperty("Int64").GetInt64());
    }

    [Fact(Skip = "BigQuery does not support JSON equality - needs TO_JSON_STRING translation")]
    public void Literal_document()
    {
        using var ctx = CreateContext();

        var doc = JsonDocument.Parse("""{ "Name": "Test customer", "Age": 80 }""");
        Assert.Empty(ctx.JsonEntities.Where(e => e.CustomerDocument == doc));

        AssertSql(
            """
            @__doc_0='{ "Name": "Test customer", "Age": 80 }'

            SELECT `j`.`Id`, `j`.`CustomerDocument`, `j`.`CustomerElement`
            FROM `JsonEntities` AS `j`
            WHERE TO_JSON_STRING(`j`.`CustomerDocument`) = TO_JSON_STRING(@__doc_0)
            """);
    }

    [Fact(Skip = "BigQuery does not support JSON equality - needs TO_JSON_STRING translation")]
    public void Parameter_document()
    {
        using var ctx = CreateContext();

        var expected = ctx.JsonEntities.Find(1).CustomerDocument;
        var actual = ctx.JsonEntities.Single(e => e.CustomerDocument == expected).CustomerDocument;

        Assert.NotNull(actual);
        Assert.Equal(expected.RootElement.GetProperty("Name").GetString(), actual.RootElement.GetProperty("Name").GetString());
    }

    [Fact]
    public void Text_output_on_document()
    {
        using var ctx = CreateContext();

        var x = ctx.JsonEntities.Single(e => e.CustomerDocument.RootElement.GetProperty("Name").GetString() == "Joe");

        Assert.Equal("Joe", x.CustomerElement.GetProperty("Name").GetString());

        AssertSql(
            """
            SELECT `j`.`Id`, `j`.`CustomerDocument`, `j`.`CustomerElement`
            FROM `JsonEntities` AS `j`
            WHERE STRING(`j`.`CustomerDocument`.Name) = 'Joe'
            LIMIT 2
            """);
    }

    [Fact]
    public void Text_output()
    {
        using var ctx = CreateContext();

        var x = ctx.JsonEntities.Single(e => e.CustomerElement.GetProperty("Name").GetString() == "Joe");

        Assert.Equal("Joe", x.CustomerElement.GetProperty("Name").GetString());

        AssertSql(
            """
            SELECT `j`.`Id`, `j`.`CustomerDocument`, `j`.`CustomerElement`
            FROM `JsonEntities` AS `j`
            WHERE STRING(`j`.`CustomerElement`.Name) = 'Joe'
            LIMIT 2
            """);
    }

    [Fact]
    public void Integer_output()
    {
        using var ctx = CreateContext();

        var x = ctx.JsonEntities.Single(e => e.CustomerElement.GetProperty("Age").GetInt32() == 25);

        Assert.Equal("Joe", x.CustomerElement.GetProperty("Name").GetString());

        AssertSql(
            """
            SELECT `j`.`Id`, `j`.`CustomerDocument`, `j`.`CustomerElement`
            FROM `JsonEntities` AS `j`
            WHERE INT64(`j`.`CustomerElement`.Age) = 25
            LIMIT 2
            """);
    }

    [Fact]
    public void Nested_property()
    {
        using var ctx = CreateContext();

        var x = ctx.JsonEntities.Single(
            e => e.CustomerElement.GetProperty("Statistics").GetProperty("Visits").GetInt64() == 4);

        Assert.Equal("Joe", x.CustomerElement.GetProperty("Name").GetString());

        AssertSql(
            """
            SELECT `j`.`Id`, `j`.`CustomerDocument`, `j`.`CustomerElement`
            FROM `JsonEntities` AS `j`
            WHERE INT64(`j`.`CustomerElement`.Statistics.Visits) = 4
            LIMIT 2
            """);
    }

    [Fact]
    public void Nested_property_twice()
    {
        using var ctx = CreateContext();

        var x = ctx.JsonEntities.Single(
            e => e.CustomerElement.GetProperty("Statistics").GetProperty("Nested").GetProperty("SomeProperty").GetInt32() == 10);

        Assert.Equal("Joe", x.CustomerElement.GetProperty("Name").GetString());

        AssertSql(
            """
            SELECT `j`.`Id`, `j`.`CustomerDocument`, `j`.`CustomerElement`
            FROM `JsonEntities` AS `j`
            WHERE INT64(`j`.`CustomerElement`.Statistics.Nested.SomeProperty) = 10
            LIMIT 2
            """);
    }

    [Fact]
    public void Array_element_access()
    {
        using var ctx = CreateContext();

        var x = ctx.JsonEntities.Single(
            e => e.CustomerElement.GetProperty("Orders")[0].GetProperty("Price").GetDouble() == 99.5);

        Assert.Equal("Joe", x.CustomerElement.GetProperty("Name").GetString());

        AssertSql(
            """
            SELECT `j`.`Id`, `j`.`CustomerDocument`, `j`.`CustomerElement`
            FROM `JsonEntities` AS `j`
            WHERE FLOAT64(`j`.`CustomerElement`.Orders[0].Price) = 99.5
            LIMIT 2
            """);
    }

    [Fact]
    public void Array_element_with_nested_property()
    {
        using var ctx = CreateContext();

        var x = ctx.JsonEntities.Single(
            e => e.CustomerElement.GetProperty("Statistics").GetProperty("Nested").GetProperty("IntArray")[1].GetInt32() == 4);

        Assert.Equal("Joe", x.CustomerElement.GetProperty("Name").GetString());

        AssertSql(
            """
            SELECT `j`.`Id`, `j`.`CustomerDocument`, `j`.`CustomerElement`
            FROM `JsonEntities` AS `j`
            WHERE INT64(`j`.`CustomerElement`.Statistics.Nested.IntArray[1]) = 4
            LIMIT 2
            """);
    }

    [Fact]
    public void Array_parameter_index()
    {
        using var ctx = CreateContext();

        var i = 1;
        var x = ctx.JsonEntities.Single(
            e => e.CustomerElement.GetProperty("Statistics").GetProperty("Nested").GetProperty("IntArray")[i].GetInt32() == 4);

        Assert.Equal("Joe", x.CustomerElement.GetProperty("Name").GetString());

        AssertSql(
            """
            @__i_0='1'

            SELECT `j`.`Id`, `j`.`CustomerDocument`, `j`.`CustomerElement`
            FROM `JsonEntities` AS `j`
            WHERE INT64(`j`.`CustomerElement`.Statistics.Nested.IntArray[@__i_0]) = 4
            LIMIT 2
            """);
    }

    [Fact]
    public void Like()
    {
        using var ctx = CreateContext();

        var x = ctx.JsonEntities.Single(e => e.CustomerElement.GetProperty("Name").GetString().StartsWith("J"));

        Assert.Equal("Joe", x.CustomerElement.GetProperty("Name").GetString());

        AssertSql(
            """
            SELECT `j`.`Id`, `j`.`CustomerDocument`, `j`.`CustomerElement`
            FROM `JsonEntities` AS `j`
            WHERE STRING(`j`.`CustomerElement`.Name) LIKE 'J%'
            LIMIT 2
            """);
    }

    private JsonDomQueryContext CreateContext() => Fixture.CreateContext();

    private void AssertSql(params string[] expected)
        => Fixture.TestSqlLoggerFactory.AssertBaseline(expected);

    #region Context and Fixture

    public class JsonDomQueryContext : PoolableDbContext
    {
        public DbSet<JsonEntity> JsonEntities { get; set; }

        public JsonDomQueryContext(DbContextOptions options) : base(options) { }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);

            modelBuilder.Entity<JsonEntity>(entity =>
            {
                entity.HasKey(e => e.Id);

                // Configure JSON properties
                entity.Property(e => e.CustomerDocument)
                    .HasColumnType("JSON");

                entity.Property(e => e.CustomerElement)
                    .HasColumnType("JSON");
            });
        }

        public static async Task SeedAsync(JsonDomQueryContext context)
        {
            var customer1 = CreateCustomer1();
            var customer2 = CreateCustomer2();

            await context.JsonEntities.AddRangeAsync(
                new JsonEntity { Id = 1, CustomerDocument = customer1, CustomerElement = customer1.RootElement },
                new JsonEntity { Id = 2, CustomerDocument = customer2, CustomerElement = customer2.RootElement }
            );

            await context.SaveChangesAsync();

            static JsonDocument CreateCustomer1()
                => JsonDocument.Parse("""
                {
                    "ID": "00000000-0000-0000-0000-000000000000",
                    "Age": 25,
                    "Name": "Joe",
                    "IsVip": false,
                    "Statistics": {
                        "Visits": 4,
                        "Purchases": 3,
                        "Nested": {
                            "SomeProperty": 10,
                            "IntArray": [3, 4]
                        }
                    },
                    "Orders": [
                        {
                            "Price": 99.5,
                            "ShippingAddress": "Some address 1"
                        },
                        {
                            "Price": 23,
                            "ShippingAddress": "Some address 2"
                        }
                    ],
                    "VariousTypes": {
                        "String": "foo",
                        "Int32": 8,
                        "Int64": 8,
                        "Bool": false
                    }
                }
                """);

            static JsonDocument CreateCustomer2()
                => JsonDocument.Parse("""
                {
                    "Age": 35,
                    "Name": "Moe",
                    "ID": "3272b593-bfe2-4ecf-81ae-4242b0632465",
                    "IsVip": true,
                    "Statistics": {
                        "Visits": 20,
                        "Purchases": 25,
                        "Nested": {
                            "SomeProperty": 20,
                            "IntArray": [5, 6, 7]
                        }
                    },
                    "Orders": [
                        {
                            "Price": 5,
                            "ShippingAddress": "Moe's address"
                        }
                    ],
                    "VariousTypes": {
                        "String": "bar",
                        "Int32": 9,
                        "Int64": 9,
                        "Bool": true
                    }
                }
                """);
        }
    }

    public class JsonEntity
    {
        public int Id { get; set; }

        [Column(TypeName = "JSON")]
        public JsonDocument CustomerDocument { get; set; }

        [Column(TypeName = "JSON")]
        public JsonElement CustomerElement { get; set; }
    }

    public class JsonDomQueryFixture : SharedStoreFixtureBase<JsonDomQueryContext>
    {
        protected override string StoreName => "JsonDomQueryTest";

        protected override ITestStoreFactory TestStoreFactory
            => BigQueryTestStoreFactory.Instance;

        public TestSqlLoggerFactory TestSqlLoggerFactory
            => (TestSqlLoggerFactory)ListLoggerFactory;

        protected override Task SeedAsync(JsonDomQueryContext context)
            => JsonDomQueryContext.SeedAsync(context);
    }

    #endregion
}
