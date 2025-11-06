using Microsoft.EntityFrameworkCore.Metadata.Conventions;
using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;

namespace Ivy.EntityFrameworkCore.BigQuery.Metadata.Conventions;

public class BigQueryConventionSetBuilder : RelationalConventionSetBuilder
{
    private readonly IRelationalTypeMappingSource _typeMappingSource;

    public BigQueryConventionSetBuilder(
        ProviderConventionSetBuilderDependencies dependencies,
        RelationalConventionSetBuilderDependencies relationalDependencies,
        IRelationalTypeMappingSource typeMappingSource)
        : base(dependencies, relationalDependencies)
    {
        _typeMappingSource = typeMappingSource;

    }

    public override ConventionSet CreateConventionSet()
    {
        var conventionSet = base.CreateConventionSet();

        return conventionSet;
    }
}
