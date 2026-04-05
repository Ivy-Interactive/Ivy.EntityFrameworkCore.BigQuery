using Ivy.EntityFrameworkCore.BigQuery.TestUtilities;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit.Abstractions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query;

public class InheritanceRelationshipsQueryBigQueryTest
    : InheritanceRelationshipsQueryRelationalTestBase<InheritanceRelationshipsQueryBigQueryTest.InheritanceRelationshipsQueryBigQueryFixture>
{
    // ReSharper disable once UnusedParameter.Local
    public InheritanceRelationshipsQueryBigQueryTest(
        InheritanceRelationshipsQueryBigQueryFixture fixture,
        ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
        Fixture.TestSqlLoggerFactory.Clear();
        Fixture.TestSqlLoggerFactory.SetTestOutputHelper(testOutputHelper);
    }

    private void AssertSql(params string[] expected)
        => Fixture.TestSqlLoggerFactory.AssertBaseline(expected);

    [ConditionalTheory(Skip = "BigQuery split query returns no results for nested include on non-entity base")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Nested_include_collection_reference_on_non_entity_base_split(bool async)
        => base.Nested_include_collection_reference_on_non_entity_base_split(async);

    public class InheritanceRelationshipsQueryBigQueryFixture : InheritanceRelationshipsQueryRelationalFixture
    {
        protected override ITestStoreFactory TestStoreFactory
            => BigQueryTestStoreFactory.Instance;
    }
}