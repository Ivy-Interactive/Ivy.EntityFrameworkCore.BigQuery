using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;
using NetTopologySuite.Features;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit.Abstractions;

namespace Ivy.EntityFrameworkCore.BigQuery.FunctionalTests.Query;

public class NorthwindAggregateOperatorsQueryBigQueryTest : NorthwindAggregateOperatorsQueryRelationalTestBase<
    NorthwindQueryBigQueryFixture<NoopModelCustomizer>>
{
    // ReSharper disable once UnusedParameter.Local
    public NorthwindAggregateOperatorsQueryBigQueryTest(
        NorthwindQueryBigQueryFixture<NoopModelCustomizer> fixture,
        ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
        ClearLog();
        Fixture.TestSqlLoggerFactory.SetTestOutputHelper(testOutputHelper);
    }
    private void AssertSql(params string[] expected)
    => Fixture.TestSqlLoggerFactory.AssertBaseline(expected);

    protected override void ClearLog()
        => Fixture.TestSqlLoggerFactory.Clear();
}