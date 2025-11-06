using Ivy.EntityFrameworkCore.BigQuery.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.EntityFrameworkCore.Update;

namespace Ivy.EntityFrameworkCore.BigQuery.FunctionalTests.Update;

public class NonSharedModelUpdatesBigQueryTest : NonSharedModelUpdatesTestBase
{
    protected override ITestStoreFactory TestStoreFactory
        => BigQueryTestStoreFactory.Instance;
}


