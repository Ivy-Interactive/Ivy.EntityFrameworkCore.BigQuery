using AdoNet.Specification.Tests;

namespace Ivy.Data.BigQuery.Conformance.Tests;

public class ParameterTests : ParameterTestBase<DbFactoryFixture>
{
    public ParameterTests(DbFactoryFixture fixture)
        : base(fixture)
    {
    }
}