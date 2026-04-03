using Ivy.EntityFrameworkCore.BigQuery.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestModels.Operators;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace Ivy.EntityFrameworkCore.BigQuery.Query;

public class OperatorsQueryBigQueryTest(NonSharedFixture fixture) : OperatorsQueryTestBase(fixture)
{
    protected override ITestStoreFactory TestStoreFactory
        => BigQueryTestStoreFactory.Instance;

    protected void AssertSql(params string[] expected)
        => TestSqlLoggerFactory.AssertBaseline(expected);

    #region Skipped: BigQuery does not support auto-generated keys

    [ConditionalTheory(Skip = "BigQuery does not support auto-generated keys - test creates entities without explicit Id")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Concat_and_json_scalar(bool _)
        => Task.CompletedTask;

    #endregion

    protected override async Task Seed(OperatorsContext ctx)
    {
        ctx.Set<OperatorEntityString>().AddRange(ExpectedData.OperatorEntitiesString);
        ctx.Set<OperatorEntityInt>().AddRange(ExpectedData.OperatorEntitiesInt);
        ctx.Set<OperatorEntityNullableInt>().AddRange(ExpectedData.OperatorEntitiesNullableInt);
        ctx.Set<OperatorEntityLong>().AddRange(ExpectedData.OperatorEntitiesLong);
        ctx.Set<OperatorEntityBool>().AddRange(ExpectedData.OperatorEntitiesBool);
        ctx.Set<OperatorEntityNullableBool>().AddRange(ExpectedData.OperatorEntitiesNullableBool);
        // Note: Not seeding DateTimeOffset as BigQuery has limited support for it

        await ctx.SaveChangesAsync();
    }
}