using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit.Abstractions;

namespace Ivy.EntityFrameworkCore.BigQuery.FunctionalTests.Query;

public class NorthwindSelectQueryBigQueryTests : NorthwindSelectQueryRelationalTestBase<NorthwindQueryBigQueryFixture<NoopModelCustomizer>>
{
    public NorthwindSelectQueryBigQueryTests(NorthwindQueryBigQueryFixture<NoopModelCustomizer> fixture, ITestOutputHelper testOutputHelper)
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
