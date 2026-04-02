using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit.Abstractions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query;

public class NorthwindAsTrackingQueryBigQueryTest : NorthwindAsTrackingQueryTestBase<NorthwindQueryBigQueryFixture<NoopModelCustomizer>>
{
    public NorthwindAsTrackingQueryBigQueryTest(
        NorthwindQueryBigQueryFixture<NoopModelCustomizer> fixture,
        ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
    }
}
