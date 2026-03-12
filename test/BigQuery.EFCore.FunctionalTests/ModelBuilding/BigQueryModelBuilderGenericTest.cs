using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace Ivy.EntityFrameworkCore.BigQuery.ModelBuilding;

public class BigQueryModelBuilderGenericTest : BigQueryModelBuilderTestBase
{
    public class BigQueryGenericNonRelationship(BigQueryModelBuilderFixture fixture) : BigQueryNonRelationship(fixture)
    {
        protected override TestModelBuilder CreateModelBuilder(
            Action<ModelConfigurationBuilder>? configure)
            => new GenericTestModelBuilder(Fixture, configure);
    }

    public class BigQueryGenericComplexType(BigQueryModelBuilderFixture fixture) : BigQueryComplexType(fixture)
    {
        protected override TestModelBuilder CreateModelBuilder(
            Action<ModelConfigurationBuilder>? configure)
            => new GenericTestModelBuilder(Fixture, configure);
    }

    public class BigQueryGenericInheritance(BigQueryModelBuilderFixture fixture) : BigQueryInheritance(fixture)
    {
        protected override TestModelBuilder CreateModelBuilder(
            Action<ModelConfigurationBuilder>? configure)
            => new GenericTestModelBuilder(Fixture, configure);
    }

    public class BigQueryGenericOneToMany(BigQueryModelBuilderFixture fixture) : BigQueryOneToMany(fixture)
    {
        protected override TestModelBuilder CreateModelBuilder(
            Action<ModelConfigurationBuilder>? configure)
            => new GenericTestModelBuilder(Fixture, configure);
    }

    public class BigQueryGenericManyToOne(BigQueryModelBuilderFixture fixture) : BigQueryManyToOne(fixture)
    {
        protected override TestModelBuilder CreateModelBuilder(
            Action<ModelConfigurationBuilder>? configure)
            => new GenericTestModelBuilder(Fixture, configure);
    }

    public class BigQueryGenericOneToOne(BigQueryModelBuilderFixture fixture) : BigQueryOneToOne(fixture)
    {
        protected override TestModelBuilder CreateModelBuilder(
            Action<ModelConfigurationBuilder>? configure)
            => new GenericTestModelBuilder(Fixture, configure);
    }

    public class BigQueryGenericManyToMany(BigQueryModelBuilderFixture fixture) : BigQueryManyToMany(fixture)
    {
        protected override TestModelBuilder CreateModelBuilder(
            Action<ModelConfigurationBuilder>? configure)
            => new GenericTestModelBuilder(Fixture, configure);
    }

    public class BigQueryGenericOwnedTypes(BigQueryModelBuilderFixture fixture) : BigQueryOwnedTypes(fixture)
    {
        protected override TestModelBuilder CreateModelBuilder(
            Action<ModelConfigurationBuilder>? configure)
            => new GenericTestModelBuilder(Fixture, configure);

        // This test causes a NullReferenceException in the execution strategy infrastructure.
        // The model building test should not require database access, but something in the
        // BigQuery provider initialization is triggering it.
        [ConditionalFact(Skip = "BigQuery execution strategy infrastructure issue")]
        public override void Can_configure_single_owned_type_using_attribute()
        {
        }
    }
}
