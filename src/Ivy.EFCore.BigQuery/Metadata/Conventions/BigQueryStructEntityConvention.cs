using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore.Metadata.Conventions;

namespace Ivy.EntityFrameworkCore.BigQuery.Metadata.Conventions;

/// <summary>
/// Makes entities with [BigQueryStruct] attribute keyless.
/// </summary>
public class BigQueryStructEntityConvention : IEntityTypeAddedConvention
{
    public void ProcessEntityTypeAdded(
        IConventionEntityTypeBuilder entityTypeBuilder,
        IConventionContext<IConventionEntityTypeBuilder> context)
    {
        var entityType = entityTypeBuilder.Metadata;

        if (HasBigQueryStructAttribute(entityType.ClrType))
        {
            entityTypeBuilder.HasNoKey();
        }
    }

    private static bool HasBigQueryStructAttribute(Type type)
    {
        var attributes = type.GetCustomAttributes(typeof(BigQueryStructAttribute), inherit: true);
        return attributes.Length > 0;
    }
}
