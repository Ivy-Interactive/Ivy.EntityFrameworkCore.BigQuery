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

        conventionSet.EntityTypeAddedConventions.Add(new BigQueryStructEntityConvention());

        // For direct JsonElement properties, set the regular BigQueryJsonTypeMapping
        // (CLR type lookup returns BigQueryOwnedJsonTypeMapping for EF's owned JSON support)
        conventionSet.PropertyAddedConventions.Add(new BigQueryJsonPropertyTypeMappingConvention());

        var structPropertyConvention = new BigQueryStructPropertyConvention(_typeMappingSource);
        conventionSet.PropertyAddedConventions.Add(structPropertyConvention);
        conventionSet.ComplexPropertyAddedConventions.Add(structPropertyConvention);
        
        conventionSet.ModelFinalizingConventions.Add(new BigQueryJsonColumnTypeConvention());

        ValueGenerationConvention valueGenerationConvention = new BigQueryValueGenerationConvention(Dependencies, RelationalDependencies);
        ReplaceConvention(conventionSet.EntityTypeBaseTypeChangedConventions, valueGenerationConvention);
        ReplaceConvention(conventionSet.EntityTypePrimaryKeyChangedConventions, valueGenerationConvention);
        ReplaceConvention(conventionSet.ForeignKeyAddedConventions, valueGenerationConvention);
        ReplaceConvention(conventionSet.ForeignKeyRemovedConventions, valueGenerationConvention);
        ReplaceConvention(conventionSet.PropertyAnnotationChangedConventions, (RelationalValueGenerationConvention)valueGenerationConvention);

        return conventionSet;
    }
}
