using Ivy.EntityFrameworkCore.BigQuery.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestModels.NullSemanticsModel;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit.Abstractions;

namespace Ivy.EntityFrameworkCore.BigQuery.FunctionalTests.Query
{
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
    }
}
