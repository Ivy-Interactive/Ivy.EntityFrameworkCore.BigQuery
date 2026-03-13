using Ivy.EntityFrameworkCore.BigQuery.TestUtilities;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace Ivy.EntityFrameworkCore.BigQuery.Query;

public class ToSqlQueryBigQueryTest : ToSqlQueryTestBase
{
    protected override ITestStoreFactory TestStoreFactory
        => BigQueryTestStoreFactory.Instance;

    [ConditionalFact]
    public virtual void Check_all_tests_overridden()
        => TestHelpers.AssertAllMethodsOverridden(GetType());

    // BigQuery doesn't support auto-increment IDs, and the raw SQL in the base test
    // uses unquoted identifiers that don't work with BigQuery's backtick quoting
    [ConditionalTheory(Skip = "BigQuery does not support auto-increment IDs")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Entity_type_with_navigation_mapped_to_SqlQuery(bool async)
        => base.Entity_type_with_navigation_mapped_to_SqlQuery(async);

    private void AssertSql(params string[] expected)
        => TestSqlLoggerFactory.AssertBaseline(expected);
}
