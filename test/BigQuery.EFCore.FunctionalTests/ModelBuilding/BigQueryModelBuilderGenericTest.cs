using Microsoft.EntityFrameworkCore;

namespace Ivy.EntityFrameworkCore.BigQuery.FunctionalTests.ModelBuilding;

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
    }
}
