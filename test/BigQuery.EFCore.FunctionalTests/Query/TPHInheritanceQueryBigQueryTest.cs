using Microsoft.EntityFrameworkCore.Query;
using Xunit.Abstractions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query;

public class TPHInheritanceQueryBigQueryTest(TPHInheritanceQueryBigQueryFixture fixture, ITestOutputHelper testOutputHelper)
    : TPHInheritanceQueryTestBase<TPHInheritanceQueryBigQueryFixture>(fixture, testOutputHelper);