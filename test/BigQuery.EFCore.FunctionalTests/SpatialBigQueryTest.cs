using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;

namespace Ivy.EntityFrameworkCore.BigQuery;

public class SpatialBigQueryTest(SpatialBigQueryFixture fixture) : SpatialTestBase<SpatialBigQueryFixture>(fixture)
{
    protected override void UseTransaction(DatabaseFacade facade, IDbContextTransaction transaction)
        => facade.UseTransaction(transaction.GetDbTransaction());

    // BigQuery doesn't support Z and M coordinates for geography, so skip this test

    public override void Can_roundtrip_Z_and_M()
    {
        // BigQuery geography type doesn't support Z and M coordinates
    }
}