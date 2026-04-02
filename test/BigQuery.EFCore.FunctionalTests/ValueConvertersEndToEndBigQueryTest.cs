using Ivy.EntityFrameworkCore.BigQuery.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace Ivy.EntityFrameworkCore.BigQuery;

public class ValueConvertersEndToEndBigQueryTest(ValueConvertersEndToEndBigQueryTest.ValueConvertersEndToEndBigQueryFixture fixture)
    : ValueConvertersEndToEndTestBase<ValueConvertersEndToEndBigQueryTest.ValueConvertersEndToEndBigQueryFixture>(fixture)
{
    public class ValueConvertersEndToEndBigQueryFixture : ValueConvertersEndToEndFixtureBase
    {
        protected override ITestStoreFactory TestStoreFactory
            => BigQueryTestStoreFactory.Instance;
    }
}
