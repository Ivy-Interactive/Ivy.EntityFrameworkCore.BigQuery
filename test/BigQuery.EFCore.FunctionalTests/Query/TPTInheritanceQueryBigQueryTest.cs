using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;
using Xunit.Abstractions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query;

public class TPTInheritanceQueryBigQueryTest(TPTInheritanceQueryBigQueryFixture fixture, ITestOutputHelper testOutputHelper)
    : TPTInheritanceQueryTestBase<TPTInheritanceQueryBigQueryFixture>(fixture, testOutputHelper)
{
    // BigQuery doesn't enforce foreign key constraints
    protected override bool EnforcesFkConstraints
        => false;

    protected override void UseTransaction(DatabaseFacade facade, IDbContextTransaction transaction)
    {
        // BigQuery doesn't support sharing transactions between contexts
    }
}