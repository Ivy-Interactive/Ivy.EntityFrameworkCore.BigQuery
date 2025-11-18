using Ivy.EntityFrameworkCore.BigQuery.TestUtilities;
using Microsoft.EntityFrameworkCore.ModelBuilding;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace Ivy.EntityFrameworkCore.BigQuery.ModelBuilding;

public class BigQueryModelBuilderTestBase : RelationalModelBuilderTest
{
    public abstract class BigQueryNonRelationship(BigQueryModelBuilderFixture fixture)
    : RelationalNonRelationshipTestBase(fixture), IClassFixture<BigQueryModelBuilderFixture>;

    public abstract class BigQueryComplexType(BigQueryModelBuilderFixture fixture)
        : RelationalComplexTypeTestBase(fixture), IClassFixture<BigQueryModelBuilderFixture>;

    public abstract class BigQueryInheritance(BigQueryModelBuilderFixture fixture)
        : RelationalInheritanceTestBase(fixture), IClassFixture<BigQueryModelBuilderFixture>;

    public abstract class BigQueryOneToMany(BigQueryModelBuilderFixture fixture)
        : RelationalOneToManyTestBase(fixture), IClassFixture<BigQueryModelBuilderFixture>;

    public abstract class BigQueryManyToOne(BigQueryModelBuilderFixture fixture)
        : RelationalManyToOneTestBase(fixture), IClassFixture<BigQueryModelBuilderFixture>;

    public abstract class BigQueryOneToOne(BigQueryModelBuilderFixture fixture)
        : RelationalOneToOneTestBase(fixture), IClassFixture<BigQueryModelBuilderFixture>;

    public abstract class BigQueryManyToMany(BigQueryModelBuilderFixture fixture)
        : RelationalManyToManyTestBase(fixture), IClassFixture<BigQueryModelBuilderFixture>;

    public abstract class BigQueryOwnedTypes(BigQueryModelBuilderFixture fixture)
        : RelationalOwnedTypesTestBase(fixture), IClassFixture<BigQueryModelBuilderFixture>;

    public class BigQueryModelBuilderFixture : RelationalModelBuilderFixture
    {
        public override TestHelpers TestHelpers
            => BigQueryTestHelpers.Instance;
    }
}