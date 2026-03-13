using Ivy.EntityFrameworkCore.BigQuery.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestModels.NullSemanticsModel;
using Xunit.Abstractions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query;

public class NullSemanticsQueryBigQueryTest : NullSemanticsQueryTestBase<NullSemanticsQueryBigQueryFixture>
{
    public NullSemanticsQueryBigQueryTest(NullSemanticsQueryBigQueryFixture fixture, ITestOutputHelper testOutputHelper) : base(fixture)
    {
    }

    protected override NullSemanticsContext CreateContext(bool useRelationalNulls = false)
    {
        var options = new DbContextOptionsBuilder(Fixture.CreateOptions());
        if (useRelationalNulls)
        {
            new BigQueryDbContextOptionsBuilder(options).UseRelationalNulls();
        }

        var context = new NullSemanticsContext(options.Options);

        context.ChangeTracker.QueryTrackingBehavior = QueryTrackingBehavior.NoTracking;

        return context;
    }

    private void AssertSql(params string[] expected)
        => Fixture.TestSqlLoggerFactory.AssertBaseline(expected);

    #region Result count differences

    // CaseOpWhen_predicate produces different row counts in BigQuery
    // due to differences in null handling semantics
    [ConditionalTheory(Skip = "BigQuery CaseOpWhen predicate produces different row count due to null handling")]
    [MemberData(nameof(IsAsyncData))]
    public override Task CaseOpWhen_predicate(bool async)
        => base.CaseOpWhen_predicate(async);

    #endregion

    #region Unsupported: NULL literal comparisons

    // BigQuery does not allow direct comparison with NULL literal
    // (Operands of = cannot be literal NULL)

    [ConditionalFact(Skip = "BigQuery does not allow = NULL comparison, must use IS NULL")]
    public override void Where_contains_on_parameter_array_with_just_null_with_relational_null_semantics()
    {
    }

    #endregion

    #region Result count differences

    // BigQuery JOIN semantics produce different row counts in some scenarios

    [ConditionalTheory(Skip = "BigQuery JOIN produces different row count due to null handling")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Join_uses_database_semantics(bool async)
        => base.Join_uses_database_semantics(async);

    #endregion
}
