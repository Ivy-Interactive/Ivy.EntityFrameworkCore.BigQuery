using Ivy.EntityFrameworkCore.BigQuery.Extensions;
using Ivy.EntityFrameworkCore.BigQuery.Infrastructure;
using Ivy.EntityFrameworkCore.BigQuery.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.Extensions.DependencyInjection;

namespace Ivy.EntityFrameworkCore.BigQuery;

public class JsonTypesBigQueryTest : JsonTypesRelationalTestBase
{
    protected override ITestStoreFactory TestStoreFactory
        => BigQueryTestStoreFactory.Instance;

    protected override IServiceCollection AddServices(IServiceCollection serviceCollection)
        => base.AddServices(serviceCollection)
            .AddEntityFrameworkBigQueryNetTopologySuite();

    #region Unsupported unsigned integer types
    [ConditionalTheory(Skip = "Unsupported type")]
    [InlineData(ushort.MinValue, """{"Prop":0}""")]
    [InlineData(ushort.MaxValue, """{"Prop":65535}""")]
    [InlineData((ushort)1, """{"Prop":1}""")]
    public override Task Can_read_write_ushort_JSON_values(ushort value, string json) => Task.CompletedTask;

    [ConditionalTheory(Skip = "Unsupported type")]
    [InlineData(uint.MinValue, """{"Prop":0}""")]
    [InlineData(uint.MaxValue, """{"Prop":4294967295}""")]
    [InlineData((uint)1, """{"Prop":1}""")]
    public override Task Can_read_write_uint_JSON_values(uint value, string json) => Task.CompletedTask;

    [ConditionalFact(Skip = "Unsupported type")]
    public override Task Can_read_write_ulong_JSON_values(ulong value, string json) => Task.CompletedTask;
    
    [ConditionalTheory(Skip = "Unsupported type")]
    [InlineData(EnumU16.Min, """{"Prop":0}""")]
    [InlineData(EnumU16.Max, """{"Prop":65535}""")]
    [InlineData(EnumU16.One, """{"Prop":1}""")]
    public override Task Can_read_write_ushort_enum_JSON_values(EnumU16 value, string json) => Task.CompletedTask;


    [ConditionalTheory(Skip = "Unsupported type")]
    [InlineData(EnumU32.Min, """{"Prop":0}""")]
    [InlineData(EnumU32.Max, """{"Prop":4294967295}""")]
    [InlineData(EnumU32.One, """{"Prop":1}""")]
    public override Task Can_read_write_uint_enum_JSON_values(EnumU32 value, string json) => Task.CompletedTask;

    [ConditionalTheory(Skip = "Unsupported type")]
    [InlineData(EnumU64.Min, """{"Prop":0}""")]
    [InlineData(EnumU64.Max, """{"Prop":18446744073709551615}""")]
    [InlineData(EnumU64.One, """{"Prop":1}""")]
    public override Task Can_read_write_ulong_enum_JSON_values(EnumU64 value, string json) => Task.CompletedTask;
    
    [ConditionalTheory(Skip = "Unsupported type")]
    [InlineData(ushort.MinValue, """{"Prop":0}""")]
    [InlineData(ushort.MaxValue, """{"Prop":65535}""")]
    [InlineData((ushort)1, """{"Prop":1}""")]
    [InlineData(null, """{"Prop":null}""")]
    public override Task Can_read_write_nullable_ushort_JSON_values(ushort? value, string json) => Task.CompletedTask;

    [ConditionalTheory(Skip = "Unsupported type")]
    [InlineData(uint.MinValue, """{"Prop":0}""")]
    [InlineData(uint.MaxValue, """{"Prop":4294967295}""")]
    [InlineData((uint)1, """{"Prop":1}""")]
    [InlineData(null, """{"Prop":null}""")]
    public override Task Can_read_write_nullable_uint_JSON_values(uint? value, string json) => Task.CompletedTask;


    [ConditionalTheory(Skip = "Unsupported type")]
    [InlineData(ulong.MinValue, """{"Prop":0}""")]
    [InlineData(ulong.MaxValue, """{"Prop":18446744073709551615}""")]
    [InlineData((ulong)1, """{"Prop":1}""")]
    [InlineData(null, """{"Prop":null}""")]
    public override Task Can_read_write_nullable_ulong_JSON_values(ulong? value, string json) => Task.CompletedTask;

    [ConditionalTheory(Skip = "Unsupported type")]
    [InlineData((ushort)EnumU16.Min, """{"Prop":0}""")]
    [InlineData((ushort)EnumU16.Max, """{"Prop":65535}""")]
    [InlineData((ushort)EnumU16.One, """{"Prop":1}""")]
    [InlineData(null, """{"Prop":null}""")]
    public override Task Can_read_write_nullable_ushort_enum_JSON_values(object? value, string json) => Task.CompletedTask;

    [ConditionalTheory(Skip = "Unsupported type")]
    [InlineData((uint)EnumU32.Min, """{"Prop":0}""")]
    [InlineData((uint)EnumU32.Max, """{"Prop":4294967295}""")]
    [InlineData((uint)EnumU32.One, """{"Prop":1}""")]
    [InlineData(null, """{"Prop":null}""")]
    public override Task Can_read_write_nullable_uint_enum_JSON_values(object? value, string json) => Task.CompletedTask;
        
    [ConditionalTheory(Skip = "Unsupported type")]
    [InlineData((ulong)EnumU64.Min, """{"Prop":0}""")]
    [InlineData((ulong)EnumU64.Max, """{"Prop":18446744073709551615}""")]
    [InlineData((ulong)EnumU64.One, """{"Prop":1}""")]
    [InlineData(null, """{"Prop":null}""")]
    public override Task Can_read_write_nullable_ulong_enum_JSON_values(object? value, string json) => Task.CompletedTask;

    [ConditionalTheory(Skip = "Unsupported type")]
    [InlineData(ushort.MinValue, """{"Prop":"0"}""")]
    [InlineData(ushort.MaxValue, """{"Prop":"65535"}""")]
    [InlineData((ushort)1, """{"Prop":"1"}""")]
    [InlineData(null, """{"Prop":null}""")]
    public override Task Can_read_write_nullable_ushort_as_string_JSON_values(ushort? value, string json) => Task.CompletedTask;

    [ConditionalTheory(Skip = "Unsupported type")]
    [InlineData(uint.MinValue, """{"Prop":"0"}""")]
    [InlineData(uint.MaxValue, """{"Prop":"4294967295"}""")]
    [InlineData((uint)1, """{"Prop":"1"}""")]
    [InlineData(null, """{"Prop":null}""")]
    public override Task Can_read_write_nullable_uint_as_string_JSON_values(uint? value, string json) => Task.CompletedTask;

    [ConditionalTheory(Skip = "Unsupported type")]
    [InlineData(ulong.MinValue, """{"Prop":"0"}""")]
    [InlineData(ulong.MaxValue, """{"Prop":"18446744073709551615"}""")]
    [InlineData((ulong)1, """{"Prop":"1"}""")]
    [InlineData(null, """{"Prop":null}""")]
    public override Task Can_read_write_nullable_ulong_as_string_JSON_values(ulong? value, string json) => Task.CompletedTask;

    [ConditionalTheory(Skip = "Unsupported type")]
    [InlineData((ushort)EnumU16.Min, """{"Prop":"Min"}""")]
    [InlineData((ushort)EnumU16.Max, """{"Prop":"Max"}""")]
    [InlineData((ushort)EnumU16.One, """{"Prop":"One"}""")]
    [InlineData((ushort)77, """{"Prop":"77"}""")]
    [InlineData(null, """{"Prop":null}""")]
    public override Task Can_read_write_nullable_ushort_enum_as_string_JSON_values(object? value, string json) => Task.CompletedTask;
    
    [ConditionalTheory(Skip = "Unsupported type")]
    [InlineData((uint)EnumU32.Min, """{"Prop":"Min"}""")]
    [InlineData((uint)EnumU32.Max, """{"Prop":"Max"}""")]
    [InlineData((uint)EnumU32.One, """{"Prop":"One"}""")]
    [InlineData((uint)77, """{"Prop":"77"}""")]
    [InlineData(null, """{"Prop":null}""")]
    public override Task Can_read_write_nullable_uint_enum_as_string_JSON_values(object? value, string json) => Task.CompletedTask;
    
    [ConditionalTheory(Skip = "Unsupported type")]
    [InlineData((ulong)EnumU64.Min, """{"Prop":"Min"}""")]
    [InlineData((ulong)EnumU64.Max, """{"Prop":"Max"}""")]
    [InlineData((ulong)EnumU64.One, """{"Prop":"One"}""")]
    [InlineData((ulong)77, """{"Prop":"77"}""")]
    [InlineData(null, """{"Prop":null}""")]
    public override Task Can_read_write_nullable_ulong_enum_as_string_JSON_values(object? value, string json) => Task.CompletedTask;

    [ConditionalFact(Skip = "Unsupported type")]
    public override Task Can_read_write_collection_of_ushort_JSON_values() => Task.CompletedTask;
    [ConditionalFact(Skip = "Unsupported type")]
    public override Task Can_read_write_collection_of_uint_JSON_values() => Task.CompletedTask;
    [ConditionalFact(Skip = "Unsupported type")]
    public override Task Can_read_write_collection_of_ulong_JSON_values() => Task.CompletedTask;
    [ConditionalFact(Skip = "Unsupported type")]
    public override Task Can_read_write_collection_of_ushort_enum_JSON_values() => Task.CompletedTask;
    [ConditionalFact(Skip = "Unsupported type")]
    public override Task Can_read_write_collection_of_uint_enum_JSON_values() => Task.CompletedTask;
    [ConditionalFact(Skip = "Unsupported type")]
    public override Task Can_read_write_collection_of_ulong_enum_JSON_values() => Task.CompletedTask;
    [ConditionalFact(Skip = "Unsupported type")]
    public override Task Can_read_write_collection_of_nullable_ushort_JSON_values() => Task.CompletedTask;
    [ConditionalFact(Skip = "Unsupported type")]
    public override Task Can_read_write_collection_of_nullable_uint_JSON_values() => Task.CompletedTask;
    [ConditionalFact(Skip = "Unsupported type")]
    public override Task Can_read_write_collection_of_nullable_ulong_JSON_values() => Task.CompletedTask;
    [ConditionalFact(Skip = "Unsupported type")]
    public override Task Can_read_write_collection_of_nullable_ushort_enum_JSON_values() => Task.CompletedTask;
    [ConditionalFact(Skip = "Unsupported type")]
    public override Task Can_read_write_collection_of_nullable_uint_enum_JSON_values() => Task.CompletedTask;
    [ConditionalFact(Skip = "Unsupported type")]
    public override Task Can_read_write_collection_of_nullable_ulong_enum_JSON_values() => Task.CompletedTask;
    [ConditionalFact(Skip = "Unsupported type")]
    public override Task Can_read_write_collection_of_ulong_values_with_converter_as_JSON_string() => Task.CompletedTask;
    [ConditionalFact(Skip = "Unsupported type")]
    public override Task Can_read_write_collection_of_ulong_enum_values_with_converter_as_JSON_string() => Task.CompletedTask;
    [ConditionalFact(Skip = "Unsupported type")]
    public override Task Can_read_write_collection_of_nullable_ulong_values_with_converter_as_JSON_string() => Task.CompletedTask;
    [ConditionalFact(Skip = "Unsupported type")]
    public override Task Can_read_write_collection_of_nullable_ulong_enum_values_with_converter_as_JSON_string() => Task.CompletedTask;
    [ConditionalFact(Skip = "Unsupported type")]
    public override Task Can_read_write_list_of_array_of_ulong_JSON_values() => Task.CompletedTask;
    [ConditionalFact(Skip = "Unsupported type")]
    public override Task Can_read_write_list_of_array_of_nullable_ulong_JSON_values() => Task.CompletedTask;
    [ConditionalFact(Skip = "Unsupported type")]
    public override Task Can_read_write_array_of_list_of_ulong_JSON_values() => Task.CompletedTask;
    [ConditionalFact(Skip = "Unsupported type")]
    public override Task Can_read_write_list_of_array_of_list_of_ulong_JSON_values() => Task.CompletedTask;

    #endregion

    #region Unsupported TimeSpan

    [ConditionalTheory(Skip = "Unsupported type")]
    [InlineData("-10675199.02:48:05.4775808", """{"Prop":"-10675199:2:48:05.4775808"}""")]
    [InlineData("10675199.02:48:05.4775807", """{"Prop":"10675199:2:48:05.4775807"}""")]
    [InlineData("00:00:00", """{"Prop":"0:00:00"}""")]
    [InlineData("12:23:23.8018854", """{"Prop":"12:23:23.8018854"}""")]
    public override Task Can_read_write_TimeSpan_JSON_values(string value, string json) => Task.CompletedTask;
    
    [ConditionalTheory(Skip = "Unsupported type")]
    [InlineData("-10675199.02:48:05.4775808", """{"Prop":"-10675199:2:48:05.4775808"}""")]
    [InlineData("10675199.02:48:05.4775807", """{"Prop":"10675199:2:48:05.4775807"}""")]
    [InlineData("00:00:00", """{"Prop":"0:00:00"}""")]
    [InlineData("12:23:23.8018854", """{"Prop":"12:23:23.8018854"}""")]
    [InlineData(null, """{"Prop":null}""")]
    public override Task Can_read_write_nullable_TimeSpan_JSON_values(string? value, string json) => Task.CompletedTask;

    [ConditionalTheory(Skip = "Unsupported type")]
    [InlineData("-10675199.02:48:05.4775808", """{"Prop":"-10675199.02:48:05.4775808"}""")]
    [InlineData("10675199.02:48:05.4775807", """{"Prop":"10675199.02:48:05.4775807"}""")]
    [InlineData("00:00:00", """{"Prop":"00:00:00"}""")]
    [InlineData("12:23:23.8018854", """{"Prop":"12:23:23.8018854"}""")]
    [InlineData(null, """{"Prop":null}""")]
    public override Task Can_read_write_nullable_TimeSpan_as_string_JSON_values(string? value, string json) => Task.CompletedTask;
    [ConditionalFact(Skip = "Unsupported type")]
    public override Task Can_read_write_collection_of_TimeSpan_JSON_values() => Task.CompletedTask;
    [ConditionalFact(Skip = "Unsupported type")]
    public override Task Can_read_write_collection_of_nullable_TimeSpan_JSON_values() => Task.CompletedTask;

    #endregion

    #region Geography - WKT format tests (working)

    [ConditionalFact]
    public override Task Can_read_write_point()
        => base.Can_read_write_point();

    [ConditionalFact]
    public override Task Can_read_write_nullable_point()
        => base.Can_read_write_nullable_point();

    [ConditionalFact]
    public override Task Can_read_write_point_with_Z()
        => base.Can_read_write_point_with_Z();

    [ConditionalFact]
    public override Task Can_read_write_point_with_M()
        => base.Can_read_write_point_with_M();

    [ConditionalFact]
    public override Task Can_read_write_point_with_Z_and_M()
        => base.Can_read_write_point_with_Z_and_M();

    [ConditionalFact]
    public override Task Can_read_write_line_string()
        => base.Can_read_write_line_string();

    [ConditionalFact]
    public override Task Can_read_write_nullable_line_string()
        => base.Can_read_write_nullable_line_string();

    [ConditionalFact]
    public override Task Can_read_write_multi_line_string()
        => base.Can_read_write_multi_line_string();

    [ConditionalFact]
    public override Task Can_read_write_nullable_multi_line_string()
        => base.Can_read_write_nullable_multi_line_string();

    [ConditionalFact]
    public override Task Can_read_write_polygon()
        => base.Can_read_write_polygon();

    [ConditionalFact]
    public override Task Can_read_write_nullable_polygon()
        => base.Can_read_write_nullable_polygon();

    [ConditionalFact]
    public override Task Can_read_write_polygon_typed_as_geometry()
        => base.Can_read_write_polygon_typed_as_geometry();

    [ConditionalFact]
    public override Task Can_read_write_polygon_typed_as_nullable_geometry()
        => base.Can_read_write_polygon_typed_as_nullable_geometry();

    #endregion

    #region Geography - GeoJson format tests (skipped due to culture bug in base test)

    // The base test's JsonGeoJsonReaderWriter uses GetDecimal() without InvariantCulture,
    // causing coordinate corruption in locales that use comma as decimal separator.
    // See: EFCore.Specification.Tests/JsonTypesTestBase.cs line 4236
    private const string SkipGeoJsonReason = "Base test JsonGeoJsonReaderWriter has culture-dependent decimal formatting bug";

    [ConditionalFact(Skip = SkipGeoJsonReason)]
    public override Task Can_read_write_point_as_GeoJson()
        => base.Can_read_write_point_as_GeoJson();

    [ConditionalFact(Skip = SkipGeoJsonReason)]
    public override Task Can_read_write_nullable_point_as_GeoJson()
        => base.Can_read_write_nullable_point_as_GeoJson();

    [ConditionalFact(Skip = SkipGeoJsonReason)]
    public override Task Can_read_write_point_with_Z_as_GeoJson()
        => base.Can_read_write_point_with_Z_as_GeoJson();

    [ConditionalFact(Skip = SkipGeoJsonReason)]
    public override Task Can_read_write_point_with_M_as_GeoJson()
        => base.Can_read_write_point_with_M_as_GeoJson();

    [ConditionalFact(Skip = SkipGeoJsonReason)]
    public override Task Can_read_write_point_with_Z_and_M_as_GeoJson()
        => base.Can_read_write_point_with_Z_and_M_as_GeoJson();

    [ConditionalFact(Skip = SkipGeoJsonReason)]
    public override Task Can_read_write_line_string_as_GeoJson()
        => base.Can_read_write_line_string_as_GeoJson();

    [ConditionalFact(Skip = SkipGeoJsonReason)]
    public override Task Can_read_write_nullable_line_string_as_GeoJson()
        => base.Can_read_write_nullable_line_string_as_GeoJson();

    [ConditionalFact(Skip = SkipGeoJsonReason)]
    public override Task Can_read_write_multi_line_string_as_GeoJson()
        => base.Can_read_write_multi_line_string_as_GeoJson();

    [ConditionalFact(Skip = SkipGeoJsonReason)]
    public override Task Can_read_write_nullable_multi_line_string_as_GeoJson()
        => base.Can_read_write_nullable_multi_line_string_as_GeoJson();

    [ConditionalFact(Skip = SkipGeoJsonReason)]
    public override Task Can_read_write_polygon_as_GeoJson()
        => base.Can_read_write_polygon_as_GeoJson();

    [ConditionalFact(Skip = SkipGeoJsonReason)]
    public override Task Can_read_write_nullable_polygon_as_GeoJson()
        => base.Can_read_write_nullable_polygon_as_GeoJson();

    [ConditionalFact(Skip = SkipGeoJsonReason)]
    public override Task Can_read_write_polygon_typed_as_geometry_as_GeoJson()
        => base.Can_read_write_polygon_typed_as_geometry_as_GeoJson();

    [ConditionalFact(Skip = SkipGeoJsonReason)]
    public override Task Can_read_write_polygon_typed_as_nullable_geometry_as_GeoJson()
        => base.Can_read_write_polygon_typed_as_nullable_geometry_as_GeoJson();

    #endregion

    protected override DbContextOptionsBuilder AddOptions(DbContextOptionsBuilder builder)
    {
        builder = base.AddOptions(builder);
        new BigQueryDbContextOptionsBuilder(builder).UseNetTopologySuite();
        return builder;
    }

    public class DecimalJsonContext : DbContext
    {
        public DecimalJsonContext(DbContextOptions options) : base(options) { }
        public DbSet<DecimalEntity> Entities { get; set; }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<DecimalEntity>(b =>
            {
                b.OwnsOne(e => e.Data, ob => ob.ToJson());
            });
        }
    }

    public class DecimalEntity
    {
        public int Id { get; set; }
        public DecimalData Data { get; set; } = new();
    }

    public class DecimalData
    {
        public decimal Prop { get; set; }
    }

    public class TestContext : DbContext
    {
        public TestContext(DbContextOptions options) : base(options) { }
        public DbSet<TestEntity> Entities { get; set; }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<TestEntity>(b =>
            {
                b.Property(e => e.JsonData).HasColumnType("JSON");
            });
        }
    }

    public class TestEntity
    {
        public int Id { get; set; }
        public string JsonData { get; set; } = "";
    }
}
