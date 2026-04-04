using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;
using Xunit.Abstractions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query;

public class TPHInheritanceQueryBigQueryTest(TPHInheritanceQueryBigQueryFixture fixture, ITestOutputHelper testOutputHelper)
    : TPHInheritanceQueryTestBase<TPHInheritanceQueryBigQueryFixture>(fixture, testOutputHelper)
{
    // BigQuery doesn't enforce foreign key constraints
    protected override bool EnforcesFkConstraints
        => false;

    protected override void UseTransaction(DatabaseFacade facade, IDbContextTransaction transaction)
    {
        // BigQuery doesn't support sharing transactions between contexts
    }
}