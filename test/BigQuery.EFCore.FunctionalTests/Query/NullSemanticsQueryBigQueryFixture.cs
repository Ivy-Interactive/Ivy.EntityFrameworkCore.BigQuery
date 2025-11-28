using Ivy.EntityFrameworkCore.BigQuery.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestModels.NullSemanticsModel;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace Ivy.EntityFrameworkCore.BigQuery.Query;

public class NullSemanticsQueryBigQueryFixture : NullSemanticsQueryFixtureBase
{
    protected override ITestStoreFactory TestStoreFactory => BigQueryTestStoreFactory.Instance;


    public override NullSemanticsContext CreateContext()
    {
        var context = base.CreateContext();
        context.ChangeTracker.QueryTrackingBehavior = QueryTrackingBehavior.NoTracking;
        return context;
    }
}