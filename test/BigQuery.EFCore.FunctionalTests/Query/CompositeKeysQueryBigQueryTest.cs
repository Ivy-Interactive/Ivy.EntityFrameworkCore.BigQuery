using Microsoft.EntityFrameworkCore.Query;
using Xunit.Abstractions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query;

public class CompositeKeysQueryBigQueryTest : CompositeKeysQueryRelationalTestBase<CompositeKeysQueryBigQueryFixture>
{
    // ReSharper disable once UnusedParameter.Local
    public CompositeKeysQueryBigQueryTest(CompositeKeysQueryBigQueryFixture fixture, ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
        Fixture.TestSqlLoggerFactory.Clear();
        Fixture.TestSqlLoggerFactory.SetTestOutputHelper(testOutputHelper);
    }

    private void AssertSql(params string[] expected)
        => Fixture.TestSqlLoggerFactory.AssertBaseline(expected);
}