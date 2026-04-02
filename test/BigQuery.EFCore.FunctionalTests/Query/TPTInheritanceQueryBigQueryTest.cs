using Microsoft.EntityFrameworkCore.Query;
using Xunit.Abstractions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query;

public class TPTInheritanceQueryBigQueryTest(TPTInheritanceQueryBigQueryFixture fixture, ITestOutputHelper testOutputHelper)
    : TPTInheritanceQueryTestBase<TPTInheritanceQueryBigQueryFixture>(fixture, testOutputHelper);