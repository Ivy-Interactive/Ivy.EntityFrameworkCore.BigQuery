#pragma warning disable EF8001 // ToJson on owned entities is obsolete

using Ivy.EntityFrameworkCore.BigQuery.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit.Sdk;

namespace Ivy.EntityFrameworkCore.BigQuery.Query;

#nullable disable

public class AdHocJsonQueryBigQueryTest(NonSharedFixture fixture) : AdHocJsonQueryRelationalTestBase(fixture)
{
    protected override ITestStoreFactory TestStoreFactory
        => BigQueryTestStoreFactory.Instance;

    // BigQuery JSON column type
    protected override string JsonColumnType => "JSON";

    #region BadJsonProperties

    // BigQuery stores JSON properly, which doesn't allow badly-formed JSON; so the following tests are irrelevant.

    public override async Task Bad_json_properties_duplicated_navigations(bool noTracking)
    {
        if (noTracking)
        {
            await Assert.ThrowsAsync<NotSupportedException>(() => base.Bad_json_properties_duplicated_navigations(noTracking: true));
        }
        else
        {
            await base.Bad_json_properties_duplicated_navigations(noTracking: false);
        }
    }

    public override Task Bad_json_properties_duplicated_scalars(bool noTracking)
        => Assert.ThrowsAsync<NotSupportedException>(() => base.Bad_json_properties_duplicated_scalars(noTracking));

    public override Task Bad_json_properties_empty_navigations(bool noTracking)
        => Assert.ThrowsAsync<NotSupportedException>(() => base.Bad_json_properties_empty_navigations(noTracking));

    public override Task Bad_json_properties_empty_scalars(bool noTracking)
        => Assert.ThrowsAsync<NotSupportedException>(() => base.Bad_json_properties_empty_scalars(noTracking));

    public override Task Bad_json_properties_null_navigations(bool noTracking)
        => Assert.ThrowsAsync<ThrowsAnyException>(() => base.Bad_json_properties_null_navigations(noTracking));

    public override Task Bad_json_properties_null_scalars(bool noTracking)
        => Assert.ThrowsAsync<ThrowsAnyException>(() => base.Bad_json_properties_null_scalars(noTracking));

    protected override Task SeedBadJsonProperties(ContextBadJsonProperties ctx)
        => throw new NotSupportedException("BigQuery stores JSON properly, doesn't allow badly-formed JSON");

    #endregion BadJsonProperties

    protected override async Task Seed21006(Context21006 context)
    {
        await base.Seed21006(context);

        // missing scalar on top level (Id = 2)
        await context.Database.ExecuteSqlAsync(
            $$$"""
INSERT INTO `Entities` (`Collection`, `OptionalReference`, `RequiredReference`, `Id`, `Name`)
VALUES (
PARSE_JSON('[{"Number":7,"Text":"e2 c1","NestedCollection":[{"DoB":"2000-01-01T00:00:00","Text":"e2 c1 c1"},{"DoB":"2000-01-01T00:00:00","Text":"e2 c1 c2"}],"NestedOptionalReference":{"DoB":"2000-01-01T00:00:00","Text":"e2 c1 nor"},"NestedRequiredReference":{"DoB":"2000-01-01T00:00:00","Text":"e2 c1 nrr"}},{"Number":7,"Text":"e2 c2","NestedCollection":[{"DoB":"2000-01-01T00:00:00","Text":"e2 c2 c1"},{"DoB":"2000-01-01T00:00:00","Text":"e2 c2 c2"}],"NestedOptionalReference":{"DoB":"2000-01-01T00:00:00","Text":"e2 c2 nor"},"NestedRequiredReference":{"DoB":"2000-01-01T00:00:00","Text":"e2 c2 nrr"}}]'),
PARSE_JSON('{"Text":"e2 or","NestedCollection":[{"DoB":"2000-01-01T00:00:00","Text":"e2 or c1"},{"DoB":"2000-01-01T00:00:00","Text":"e2 or c2"}],"NestedOptionalReference":{"DoB":"2000-01-01T00:00:00","Text":"e2 or nor"},"NestedRequiredReference":{"DoB":"2000-01-01T00:00:00","Text":"e2 or nrr"}}'),
PARSE_JSON('{"Text":"e2 rr","NestedCollection":[{"DoB":"2000-01-01T00:00:00","Text":"e2 rr c1"},{"DoB":"2000-01-01T00:00:00","Text":"e2 rr c2"}],"NestedOptionalReference":{"DoB":"2000-01-01T00:00:00","Text":"e2 rr nor"},"NestedRequiredReference":{"DoB":"2000-01-01T00:00:00","Text":"e2 rr nrr"}}'),
2,
'e2')
""");

        // missing scalar on nested level (Id = 3)
        await context.Database.ExecuteSqlAsync(
            $$$"""
INSERT INTO `Entities` (`Collection`, `OptionalReference`, `RequiredReference`, `Id`, `Name`)
VALUES (
PARSE_JSON('[{"Number":7,"Text":"e3 c1","NestedCollection":[{"Text":"e3 c1 c1"},{"Text":"e3 c1 c2"}],"NestedOptionalReference":{"Text":"e3 c1 nor"},"NestedRequiredReference":{"Text":"e3 c1 nrr"}},{"Number":7,"Text":"e3 c2","NestedCollection":[{"Text":"e3 c2 c1"},{"Text":"e3 c2 c2"}],"NestedOptionalReference":{"Text":"e3 c2 nor"},"NestedRequiredReference":{"Text":"e3 c2 nrr"}}]'),
PARSE_JSON('{"Number":7,"Text":"e3 or","NestedCollection":[{"Text":"e3 or c1"},{"Text":"e3 or c2"}],"NestedOptionalReference":{"Text":"e3 or nor"},"NestedRequiredReference":{"Text":"e3 or nrr"}}'),
PARSE_JSON('{"Number":7,"Text":"e3 rr","NestedCollection":[{"Text":"e3 rr c1"},{"Text":"e3 rr c2"}],"NestedOptionalReference":{"Text":"e3 rr nor"},"NestedRequiredReference":{"Text":"e3 rr nrr"}}'),
3,
'e3')
""");

        // null scalar on top level (Id = 4)
        await context.Database.ExecuteSqlAsync(
            $$$"""
INSERT INTO `Entities` (`Collection`, `OptionalReference`, `RequiredReference`, `Id`, `Name`)
VALUES (
PARSE_JSON('[{"Number":null,"Text":"e4 c1","NestedCollection":[{"Text":"e4 c1 c1"},{"Text":"e4 c1 c2"}],"NestedOptionalReference":{"Text":"e4 c1 nor"},"NestedRequiredReference":{"Text":"e4 c1 nrr"}},{"Number":null,"Text":"e4 c2","NestedCollection":[{"Text":"e4 c2 c1"},{"Text":"e4 c2 c2"}],"NestedOptionalReference":{"Text":"e4 c2 nor"},"NestedRequiredReference":{"Text":"e4 c2 nrr"}}]'),
PARSE_JSON('{"Number":null,"Text":"e4 or","NestedCollection":[{"Text":"e4 or c1"},{"Text":"e4 or c2"}],"NestedOptionalReference":{"Text":"e4 or nor"},"NestedRequiredReference":{"Text":"e4 or nrr"}}'),
PARSE_JSON('{"Number":null,"Text":"e4 rr","NestedCollection":[{"Text":"e4 rr c1"},{"Text":"e4 rr c2"}],"NestedOptionalReference":{"Text":"e4 rr nor"},"NestedRequiredReference":{"Text":"e4 rr nrr"}}'),
4,
'e4')
""");

        // missing required navigation (Id = 5)
        await context.Database.ExecuteSqlAsync(
            $$$"""
INSERT INTO `Entities` (`Collection`, `OptionalReference`, `RequiredReference`, `Id`, `Name`)
VALUES (
PARSE_JSON('[{"Number":7,"Text":"e5 c1","NestedCollection":[{"DoB":"2000-01-01T00:00:00","Text":"e5 c1 c1"},{"DoB":"2000-01-01T00:00:00","Text":"e5 c1 c2"}],"NestedOptionalReference":{"DoB":"2000-01-01T00:00:00","Text":"e5 c1 nor"}},{"Number":7,"Text":"e5 c2","NestedCollection":[{"DoB":"2000-01-01T00:00:00","Text":"e5 c2 c1"},{"DoB":"2000-01-01T00:00:00","Text":"e5 c2 c2"}],"NestedOptionalReference":{"DoB":"2000-01-01T00:00:00","Text":"e5 c2 nor"}}]'),
PARSE_JSON('{"Number":7,"Text":"e5 or","NestedCollection":[{"DoB":"2000-01-01T00:00:00","Text":"e5 or c1"},{"DoB":"2000-01-01T00:00:00","Text":"e5 or c2"}],"NestedOptionalReference":{"DoB":"2000-01-01T00:00:00","Text":"e5 or nor"}}'),
PARSE_JSON('{"Number":7,"Text":"e5 rr","NestedCollection":[{"DoB":"2000-01-01T00:00:00","Text":"e5 rr c1"},{"DoB":"2000-01-01T00:00:00","Text":"e5 rr c2"}],"NestedOptionalReference":{"DoB":"2000-01-01T00:00:00","Text":"e5 rr nor"}}'),
5,
'e5')
""");

        // null required navigation (Id = 6)
        await context.Database.ExecuteSqlAsync(
            $$$"""
INSERT INTO `Entities` (`Collection`, `OptionalReference`, `RequiredReference`, `Id`, `Name`)
VALUES (
PARSE_JSON('[{"Number":7,"Text":"e6 c1","NestedCollection":[{"DoB":"2000-01-01T00:00:00","Text":"e6 c1 c1"},{"DoB":"2000-01-01T00:00:00","Text":"e6 c1 c2"}],"NestedOptionalReference":{"DoB":"2000-01-01T00:00:00","Text":"e6 c1 nor"},"NestedRequiredReference":null},{"Number":7,"Text":"e6 c2","NestedCollection":[{"DoB":"2000-01-01T00:00:00","Text":"e6 c2 c1"},{"DoB":"2000-01-01T00:00:00","Text":"e6 c2 c2"}],"NestedOptionalReference":{"DoB":"2000-01-01T00:00:00","Text":"e6 c2 nor"},"NestedRequiredReference":null}]'),
PARSE_JSON('{"Number":7,"Text":"e6 or","NestedCollection":[{"DoB":"2000-01-01T00:00:00","Text":"e6 or c1"},{"DoB":"2000-01-01T00:00:00","Text":"e6 or c2"}],"NestedOptionalReference":{"DoB":"2000-01-01T00:00:00","Text":"e6 or nor"},"NestedRequiredReference":null}'),
PARSE_JSON('{"Number":7,"Text":"e6 rr","NestedCollection":[{"DoB":"2000-01-01T00:00:00","Text":"e6 rr c1"},{"DoB":"2000-01-01T00:00:00","Text":"e6 rr c2"}],"NestedOptionalReference":{"DoB":"2000-01-01T00:00:00","Text":"e6 rr nor"},"NestedRequiredReference":null}'),
6,
'e6')
""");
    }

    protected override async Task Seed29219(DbContext ctx)
    {
        // Entity 1: Complete data with all scalars
        await ctx.Database.ExecuteSqlAsync(
            $$"""
INSERT INTO `Entities` (`Id`, `Reference`, `Collection`)
VALUES(1, PARSE_JSON('{"NonNullableScalar":10,"NullableScalar":11}'), PARSE_JSON('[{"NonNullableScalar":100,"NullableScalar":101},{"NonNullableScalar":200,"NullableScalar":201},{"NonNullableScalar":300,"NullableScalar":null}]'))
""");

        // Entity 2: NullableScalar is null in reference
        await ctx.Database.ExecuteSqlAsync(
            $$"""
INSERT INTO `Entities` (`Id`, `Reference`, `Collection`)
VALUES(2, PARSE_JSON('{"NonNullableScalar":20,"NullableScalar":null}'), PARSE_JSON('[{"NonNullableScalar":1001,"NullableScalar":null}]'))
""");

        // Entity 3: Missing NullableScalar in JSON (tests missing properties)
        await ctx.Database.ExecuteSqlAsync(
            $$"""
INSERT INTO `Entities` (`Id`, `Reference`, `Collection`)
VALUES(3, PARSE_JSON('{"NonNullableScalar":30}'), PARSE_JSON('[{"NonNullableScalar":10001}]'))
""");
    }

    protected override async Task Seed30028(DbContext ctx)
    {
        // complete
        await ctx.Database.ExecuteSqlAsync(
            $$$$"""
INSERT INTO `Entities` (`Id`, `Json`)
VALUES(
1,
PARSE_JSON('{"RootName":"e1","Collection":[{"BranchName":"e1 c1","Nested":{"LeafName":"e1 c1 l"}},{"BranchName":"e1 c2","Nested":{"LeafName":"e1 c2 l"}}],"OptionalReference":{"BranchName":"e1 or","Nested":{"LeafName":"e1 or l"}},"RequiredReference":{"BranchName":"e1 rr","Nested":{"LeafName":"e1 rr l"}}}'))
""");

        // missing collection
        await ctx.Database.ExecuteSqlAsync(
            $$$$"""
INSERT INTO `Entities` (`Id`, `Json`)
VALUES(
2,
PARSE_JSON('{"RootName":"e2","OptionalReference":{"BranchName":"e2 or","Nested":{"LeafName":"e2 or l"}},"RequiredReference":{"BranchName":"e2 rr","Nested":{"LeafName":"e2 rr l"}}}'))
""");

        // missing optional reference
        await ctx.Database.ExecuteSqlAsync(
            $$$$"""
INSERT INTO `Entities` (`Id`, `Json`)
VALUES(
3,
PARSE_JSON('{"RootName":"e3","Collection":[{"BranchName":"e3 c1","Nested":{"LeafName":"e3 c1 l"}},{"BranchName":"e3 c2","Nested":{"LeafName":"e3 c2 l"}}],"RequiredReference":{"BranchName":"e3 rr","Nested":{"LeafName":"e3 rr l"}}}'))
""");

        // missing required reference
        await ctx.Database.ExecuteSqlAsync(
            $$$$"""
INSERT INTO `Entities` (`Id`, `Json`)
VALUES(
4,
PARSE_JSON('{"RootName":"e4","Collection":[{"BranchName":"e4 c1","Nested":{"LeafName":"e4 c1 l"}},{"BranchName":"e4 c2","Nested":{"LeafName":"e4 c2 l"}}],"OptionalReference":{"BranchName":"e4 or","Nested":{"LeafName":"e4 or l"}}}'))
""");
    }

    protected override async Task Seed33046(DbContext ctx)
        => await ctx.Database.ExecuteSqlAsync(
            $$"""
INSERT INTO `Reviews` (`Rounds`, `Id`)
VALUES(PARSE_JSON('[{"RoundNumber":11,"SubRounds":[{"SubRoundNumber":111},{"SubRoundNumber":112}]}]'), 1)
""");

    protected override async Task SeedJunkInJson(DbContext ctx)
        => await ctx.Database.ExecuteSqlAsync(
            $$$"""
INSERT INTO `Entities` (`Collection`, `CollectionWithCtor`, `Reference`, `ReferenceWithCtor`, `Id`)
VALUES(
PARSE_JSON('[{"JunkReference":{"Something":"SomeValue" },"Name":"c11","JunkProperty1":50,"Number":11.5,"JunkCollection1":[],"JunkCollection2":[{"Foo":"junk value"}],"NestedCollection":[{"DoB":"2002-04-01T00:00:00","DummyProp":"Dummy value"},{"DoB":"2002-04-02T00:00:00","DummyReference":{"Foo":5}}],"NestedReference":{"DoB":"2002-03-01T00:00:00"}},{"Name":"c12","Number":12.5,"NestedCollection":[{"DoB":"2002-06-01T00:00:00"},{"DoB":"2002-06-02T00:00:00"}],"NestedDummy":59,"NestedReference":{"DoB":"2002-05-01T00:00:00"}}]'),
PARSE_JSON('[{"MyBool":true,"Name":"c11 ctor","JunkReference":{"Something":"SomeValue","JunkCollection":[{"Foo":"junk value"}]},"NestedCollection":[{"DoB":"2002-08-01T00:00:00"},{"DoB":"2002-08-02T00:00:00"}],"NestedReference":{"DoB":"2002-07-01T00:00:00"}},{"MyBool":false,"Name":"c12 ctor","NestedCollection":[{"DoB":"2002-10-01T00:00:00"},{"DoB":"2002-10-02T00:00:00"}],"JunkCollection":[{"Foo":"junk value"}],"NestedReference":{"DoB":"2002-09-01T00:00:00"}}]'),
PARSE_JSON('{"Name":"r1","JunkCollection":[{"Foo":"junk value"}],"JunkReference":{"Something":"SomeValue" },"Number":1.5,"NestedCollection":[{"DoB":"2000-02-01T00:00:00","JunkReference":{"Something":"SomeValue"}},{"DoB":"2000-02-02T00:00:00"}],"NestedReference":{"DoB":"2000-01-01T00:00:00"}}'),
PARSE_JSON('{"MyBool":true,"JunkCollection":[{"Foo":"junk value"}],"Name":"r1 ctor","JunkReference":{"Something":"SomeValue" },"NestedCollection":[{"DoB":"2001-02-01T00:00:00"},{"DoB":"2001-02-02T00:00:00"}],"NestedReference":{"JunkCollection":[{"Foo":"junk value"}],"DoB":"2001-01-01T00:00:00"}}'),
1)
""");

    protected override async Task SeedTrickyBuffering(DbContext ctx)
        => await ctx.Database.ExecuteSqlAsync(
            $$$"""
INSERT INTO `Entities` (`Reference`, `Id`)
VALUES(
PARSE_JSON('{"Name": "r1", "Number": 7, "JunkReference":{"Something": "SomeValue" }, "JunkCollection": [{"Foo": "junk value"}], "NestedReference": {"DoB": "2000-01-01T00:00:00"}, "NestedCollection": [{"DoB": "2000-02-01T00:00:00", "JunkReference": {"Something": "SomeValue"}}, {"DoB": "2000-02-02T00:00:00"}]}'),1)
""");

    protected override async Task SeedShadowProperties(DbContext ctx)
        => await ctx.Database.ExecuteSqlAsync(
            $$"""
INSERT INTO `Entities` (`Collection`, `CollectionWithCtor`, `Reference`, `ReferenceWithCtor`, `Id`, `Name`)
VALUES(
PARSE_JSON('[{"Name":"e1_c1","ShadowDouble":5.5},{"ShadowDouble":20.5,"Name":"e1_c2"}]'),
PARSE_JSON('[{"Name":"e1_c1 ctor","ShadowNullableByte":6},{"ShadowNullableByte":null,"Name":"e1_c2 ctor"}]'),
PARSE_JSON('{"Name":"e1_r", "ShadowString":"Foo"}'),
PARSE_JSON('{"ShadowInt":143,"Name":"e1_r ctor"}'),
1,
'e1')
""");

    protected override async Task SeedNotICollection(DbContext ctx)
    {
        await ctx.Database.ExecuteSqlAsync(
            $$"""
INSERT INTO `Entities` (`Json`, `Id`)
VALUES(
PARSE_JSON('{"Collection":[{"Bar":11,"Foo":"c11"},{"Bar":12,"Foo":"c12"},{"Bar":13,"Foo":"c13"}]}'),
1)
""");

        await ctx.Database.ExecuteSqlAsync(
            $$"""
INSERT INTO `Entities` (`Json`, `Id`)
VALUES(
PARSE_JSON('{"Collection":[{"Bar":21,"Foo":"c21"},{"Bar":22,"Foo":"c22"}]}'),
2)
""");
    }

    // Override to explicitly configure the Name property
    // EF Core 10 may not auto-discover regular properties when owned entities are mapped to JSON
    protected override void OnModelCreating21006(ModelBuilder modelBuilder)
    {
        base.OnModelCreating21006(modelBuilder);

        modelBuilder.Entity<Context21006.Entity>(b =>
        {
            b.Property(x => x.Name).HasColumnName("Name").HasColumnType("STRING");
        });
    }

    protected void AssertSql(params string[] expected)
        => TestSqlLoggerFactory.AssertBaseline(expected);
}
