using Ivy.EntityFrameworkCore.BigQuery.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace Ivy.EntityFrameworkCore.BigQuery.FunctionalTests.Query
{
    public class NullSemanticsQueryBigQueryFixture : NullSemanticsQueryFixtureBase
    {
        protected override ITestStoreFactory TestStoreFactory => BigQueryTestStoreFactory.Instance;
    }
}