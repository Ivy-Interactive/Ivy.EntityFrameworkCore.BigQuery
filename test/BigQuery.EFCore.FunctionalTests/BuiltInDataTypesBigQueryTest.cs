using Ivy.EntityFrameworkCore.BigQuery.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit.Abstractions;

namespace Ivy.EntityFrameworkCore.BigQuery;

public class BuiltInDataTypesBigQueryTest : BuiltInDataTypesTestBase<BuiltInDataTypesBigQueryTest.BuiltInDataTypesBigQueryFixture>
{
    public BuiltInDataTypesBigQueryTest(BuiltInDataTypesBigQueryFixture fixture, ITestOutputHelper testOutputHelper)
   : base(fixture)
    {
        Fixture.TestSqlLoggerFactory.Clear();
        Fixture.TestSqlLoggerFactory.SetTestOutputHelper(testOutputHelper);
    }

    public class BuiltInDataTypesBigQueryFixture : BuiltInDataTypesFixtureBase, ITestSqlLoggerFactory
    {
        public override bool StrictEquality
            => false;

        public override bool SupportsAnsi
            => false;

        public override bool SupportsUnicodeToAnsiConversion
            => true;

        public override bool SupportsLargeStringComparisons
            => true;

        public override bool SupportsDecimalComparisons
            => false;

        public override bool PreservesDateTimeKind
            => false;

        protected override ITestStoreFactory TestStoreFactory
            => BigQueryTestStoreFactory.Instance;

        public TestSqlLoggerFactory TestSqlLoggerFactory
            => (TestSqlLoggerFactory)ListLoggerFactory;

        protected override void OnModelCreating(ModelBuilder modelBuilder, DbContext context)
        {
            base.OnModelCreating(modelBuilder, context);
            //todo
        }

        public override bool SupportsBinaryKeys
            => false;

        public override DateTime DefaultDateTime
            => new();
    }
}
