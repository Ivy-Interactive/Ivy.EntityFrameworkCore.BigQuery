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

    public override Task Entity_type_with_navigation_mapped_to_SqlQuery(bool async)
        => Task.CompletedTask;

    private void AssertSql(params string[] expected)
        => TestSqlLoggerFactory.AssertBaseline(expected);
}
