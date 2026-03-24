using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Conventions;
using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;

namespace Ivy.EntityFrameworkCore.BigQuery.Metadata.Conventions;

/// <summary>
/// A convention that configures value generation for BigQuery.
/// BigQuery doesn't support auto-generated keys (like IDENTITY or SERIAL),
/// so this convention sets ValueGenerated.Never for primary key properties
/// that don't have a default value configured.
/// </summary>
public class BigQueryValueGenerationConvention : RelationalValueGenerationConvention
{
    public BigQueryValueGenerationConvention(
        ProviderConventionSetBuilderDependencies dependencies,
        RelationalConventionSetBuilderDependencies relationalDependencies)
        : base(dependencies, relationalDependencies)
    {
    }

    /// <summary>
    /// Returns the store value generation strategy to set for the given property.
    /// </summary>
    protected override ValueGenerated? GetValueGenerated(IConventionProperty property)
    {
        // For JSON-mapped entities with collection ordinal keys, allow OnAdd
        // (these are synthetic keys managed by EF Core for JSON collections)
        var declaringEntityType = property.DeclaringType as IReadOnlyEntityType;
        if (declaringEntityType != null
            && declaringEntityType.IsMappedToJson()
            && declaringEntityType.FindOwnership()?.IsUnique == false
            && property.IsPrimaryKey())
        {
            return ValueGenerated.OnAdd;
        }

        var declaringTable = property.GetMappedStoreObjects(StoreObjectType.Table).FirstOrDefault();
        if (declaringTable.Name == null)
        {
            return null;
        }

        return GetValueGenerated(property, declaringTable);
    }

    /// <summary>
    /// Returns the store value generation strategy to set for the given property.
    /// BigQuery doesn't support server-side auto-generated keys (IDENTITY/SERIAL), but
    /// we still allow client-side value generation (e.g., GUIDs).
    /// </summary>
    public static new ValueGenerated? GetValueGenerated(IReadOnlyProperty property, in StoreObjectIdentifier storeObject)
    {
        var result = RelationalValueGenerationConvention.GetValueGenerated(property, storeObject);
        if (result != null)
        {
            return result;
        }

        return null;
    }
}
