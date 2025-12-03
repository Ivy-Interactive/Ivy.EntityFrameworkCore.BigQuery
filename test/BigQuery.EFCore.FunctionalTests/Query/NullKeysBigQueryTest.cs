using Ivy.EntityFrameworkCore.BigQuery.TestUtilities;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;


namespace Ivy.EntityFrameworkCore.BigQuery.Query;

public class NullKeysBigQueryTest(NullKeysBigQueryTest.NullKeysBigQueryFixture fixture)
    : NullKeysTestBase<NullKeysBigQueryTest.NullKeysBigQueryFixture>(fixture)
{
    public class NullKeysBigQueryFixture : NullKeysFixtureBase
    {
        protected override string StoreName { get; } = "StringsContext";

        protected override ITestStoreFactory TestStoreFactory
            => BigQueryTestStoreFactory.Instance;
    }
}
