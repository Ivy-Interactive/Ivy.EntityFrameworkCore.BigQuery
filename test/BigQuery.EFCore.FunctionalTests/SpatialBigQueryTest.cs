using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.TestModels.SpatialModel;
using NetTopologySuite.Geometries;

namespace Ivy.EntityFrameworkCore.BigQuery;

public class SpatialBigQueryTest(SpatialBigQueryFixture fixture) : SpatialTestBase<SpatialBigQueryFixture>(fixture)
{
    protected override void UseTransaction(DatabaseFacade facade, IDbContextTransaction transaction)
    {
    }

    // BigQuery GEOGRAPHY normalizes polygon coordinates (reorders starting vertex per RFC 7946),
    // so we use EqualsTopologically instead of exact coordinate comparison
    public override async Task Mutation_of_tracked_values_does_not_mutate_values_in_store()
    {
        Point CreatePoint(double y = 2.2)
            => new(1.1, y, 3.3);

        Polygon CreatePolygon(double y = 2.2)
            => new(
                new LinearRing([new Coordinate(1.1, 2.2), new Coordinate(2.2, y), new Coordinate(2.2, 1.1), new Coordinate(1.1, 2.2)]));

        var id1 = Guid.NewGuid();
        var id2 = Guid.NewGuid();
        var point = CreatePoint();
        var polygon = CreatePolygon();

        await ExecuteWithStrategyInTransactionAsync(
            context =>
            {
                context.AddRange(
                    new PointEntity { Id = id1, Point = point },
                    new PolygonEntity { Id = id2, Polygon = polygon });

                return context.SaveChangesAsync();
            }, async context =>
            {
                point.X = 11.1;
                polygon.Coordinates[1].X = 11.1;

                var fromStore1 = await context.Set<PointEntity>().FirstAsync(p => p.Id == id1);
                var fromStore2 = await context.Set<PolygonEntity>().FirstAsync(p => p.Id == id2);

                Assert.True(CreatePoint().EqualsTopologically(fromStore1.Point));
                Assert.True(CreatePolygon().EqualsTopologically(fromStore2.Polygon));

                fromStore1.Point.Y = 22.2;
                fromStore2.Polygon.Coordinates[1].Y = 22.2;

                context.Entry(fromStore2).State = EntityState.Unchanged;

                await context.SaveChangesAsync();
            }, async context =>
            {
                var fromStore1 = await context.Set<PointEntity>().FirstAsync(p => p.Id == id1);
                var fromStore2 = await context.Set<PolygonEntity>().FirstAsync(p => p.Id == id2);
                
                Assert.True(CreatePoint(22.2).EqualsTopologically(fromStore1.Point));
                Assert.True(CreatePolygon().EqualsTopologically(fromStore2.Polygon));
            });
    }

    // BigQuery doesn't support Z and M coordinates for geography
    public override void Can_roundtrip_Z_and_M()
    {
    }
}