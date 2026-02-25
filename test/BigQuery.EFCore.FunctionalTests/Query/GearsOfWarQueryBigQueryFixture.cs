using Ivy.EntityFrameworkCore.BigQuery.Extensions;
using Ivy.EntityFrameworkCore.BigQuery.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestModels.GearsOfWarModel;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace Ivy.EntityFrameworkCore.BigQuery.Query;

public class GearsOfWarQueryBigQueryFixture : GearsOfWarQueryRelationalFixture
{
    static GearsOfWarQueryBigQueryFixture() { }

    protected override ITestStoreFactory TestStoreFactory => GearsOfWarBigQueryTestStoreFactory.Instance;

    private GearsOfWarData? _expectedData;

    public override ISetSource GetExpectedData()
    {
        if (_expectedData is null)
        {
            _expectedData = (GearsOfWarData)base.GetExpectedData();

            // BigQuery has no DateTimeOffset equivalent - normalize to UTC with microsecond precision
            foreach (var mission in _expectedData.Missions)
            {
                mission.Timeline = new DateTimeOffset(
                    mission.Timeline.Ticks - (mission.Timeline.Ticks % (TimeSpan.TicksPerMillisecond / 1000)), TimeSpan.Zero);
            }
        }

        return _expectedData;
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder, DbContext context)
    {
        base.OnModelCreating(modelBuilder, context);

        modelBuilder.Entity<City>().Property(g => g.Location).HasColumnType("STRING(100)");
    }

    protected override async Task SeedAsync(GearsOfWarContext context)
    {
        var squads = GearsOfWarData.CreateSquads();
        var missions = GearsOfWarData.CreateMissions();
        var squadMissions = GearsOfWarData.CreateSquadMissions();
        var cities = GearsOfWarData.CreateCities();
        var weapons = GearsOfWarData.CreateWeapons();
        var tags = GearsOfWarData.CreateTags();
        var gears = GearsOfWarData.CreateGears();
        var locustLeaders = GearsOfWarData.CreateLocustLeaders();
        var factions = GearsOfWarData.CreateFactions();
        var locustHighCommands = GearsOfWarData.CreateHighCommands();

        // BigQuery has no DateTimeOffset equivalent - normalize to UTC with microsecond precision
        foreach (var mission in missions)
        {
            mission.Timeline = new DateTimeOffset(
                mission.Timeline.Ticks - (mission.Timeline.Ticks % (TimeSpan.TicksPerMillisecond / 1000)), TimeSpan.Zero);
        }

        GearsOfWarData.WireUp(squads, missions, squadMissions, cities, weapons, tags, gears, locustLeaders, factions, locustHighCommands);

        context.Squads.AddRange(squads);
        context.Missions.AddRange(missions);
        context.SquadMissions.AddRange(squadMissions);
        context.Cities.AddRange(cities);
        context.Weapons.AddRange(weapons);
        context.Tags.AddRange(tags);
        context.Gears.AddRange(gears);
        context.LocustLeaders.AddRange(locustLeaders);
        context.Factions.AddRange(factions);
        context.LocustHighCommands.AddRange(locustHighCommands);
        await context.SaveChangesAsync();

        GearsOfWarData.WireUp2(locustLeaders, factions);

        await context.SaveChangesAsync();
    }
}

public class GearsOfWarBigQueryTestStoreFactory : BigQueryTestStoreFactory
{
    public new static GearsOfWarBigQueryTestStoreFactory Instance { get; } = new();

    public override TestStore Create(string storeName)
        => new GearsOfWarBigQueryTestStore(storeName, shared: false);

    public override TestStore GetOrCreate(string storeName)
        => new GearsOfWarBigQueryTestStore(storeName, shared: true);
}

public class GearsOfWarBigQueryTestStore : BigQueryTestStore
{
    public GearsOfWarBigQueryTestStore(string name, bool shared) : base(name, shared)
    {
    }

    public override DbContextOptionsBuilder AddProviderOptions(DbContextOptionsBuilder builder)
    {
        return builder.UseBigQuery(Connection.ConnectionString, b =>
        {
            b.IgnoreUniqueConstraints();
            b.ApplyConfiguration();
        });
    }
}