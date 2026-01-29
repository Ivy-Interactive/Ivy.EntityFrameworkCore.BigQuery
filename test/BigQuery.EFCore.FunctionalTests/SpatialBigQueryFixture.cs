using Ivy.EntityFrameworkCore.BigQuery.Extensions;
using Ivy.EntityFrameworkCore.BigQuery.Infrastructure;
using Ivy.EntityFrameworkCore.BigQuery.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestModels.SpatialModel;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.Extensions.DependencyInjection;
using NetTopologySuite;
using NetTopologySuite.Geometries;

namespace Ivy.EntityFrameworkCore.BigQuery;

public class SpatialBigQueryFixture : SpatialFixtureBase
{
    private readonly GeometryFactory _geometryFactory
        = NtsGeometryServices.Instance.CreateGeometryFactory(srid: 4326);

    protected override ITestStoreFactory TestStoreFactory
        => BigQueryTestStoreFactory.Instance;

    protected override string StoreName
        => "SpatialTest";

    protected override IServiceCollection AddServices(IServiceCollection serviceCollection)
        => base.AddServices(serviceCollection)
            .AddEntityFrameworkBigQuery()
            .AddEntityFrameworkBigQueryNetTopologySuite();

    public override DbContextOptionsBuilder AddOptions(DbContextOptionsBuilder builder)
    {
        var optionsBuilder = base.AddOptions(builder);
        new BigQueryDbContextOptionsBuilder(optionsBuilder).UseNetTopologySuite();
        return optionsBuilder;
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder, DbContext context)
    {
        base.OnModelCreating(modelBuilder, context);

        // BigQuery geography columns store as GEOGRAPHY type
        modelBuilder.Entity<PointEntity>(
            b =>
            {
                b.Property(e => e.Point).HasColumnType("GEOGRAPHY");
                b.Property(e => e.PointZ).HasColumnType("GEOGRAPHY");
                b.Property(e => e.PointM).HasColumnType("GEOGRAPHY");
                b.Property(e => e.PointZM).HasColumnType("GEOGRAPHY");
                b.Property(e => e.Geometry).HasColumnType("GEOGRAPHY");
            });

        modelBuilder.Entity<LineStringEntity>(
            b =>
            {
                b.Property(e => e.LineString).HasColumnType("GEOGRAPHY");
            });

        modelBuilder.Entity<PolygonEntity>(
            b =>
            {
                b.Property(e => e.Polygon).HasColumnType("GEOGRAPHY");
            });

        modelBuilder.Entity<MultiLineStringEntity>(
            b =>
            {
                b.Property(e => e.MultiLineString).HasColumnType("GEOGRAPHY");
            });

        modelBuilder.Entity<GeoPointEntity>(
            b =>
            {
                b.Property(e => e.Location).HasColumnType("GEOGRAPHY");
            });
    }

    protected override Task SeedAsync(SpatialContext context)
        => SpatialContext.SeedAsync(context, _geometryFactory);
}
