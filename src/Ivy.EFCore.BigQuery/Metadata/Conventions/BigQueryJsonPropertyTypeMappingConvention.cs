using Ivy.EntityFrameworkCore.BigQuery.Storage.Internal.Mapping;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore.Metadata.Conventions;
using System.Text.Json;

namespace Ivy.EntityFrameworkCore.BigQuery.Metadata.Conventions;

/// <summary>
/// Convention that sets BigQueryJsonTypeMapping for properties typed as JsonElement.
/// This is necessary because the CLR type lookup for JsonElement returns BigQueryOwnedJsonTypeMapping
/// (needed for EF Core's owned JSON support), but direct JsonElement properties require the
/// non-owned BigQueryJsonTypeMapping which uses value converters instead of MemoryStream.
/// </summary>
public class BigQueryJsonPropertyTypeMappingConvention : IPropertyAddedConvention
{
    private BigQueryJsonTypeMapping? _jsonTypeMapping;

    /// <inheritdoc />
    public void ProcessPropertyAdded(IConventionPropertyBuilder propertyBuilder, IConventionContext<IConventionPropertyBuilder> context)
    {
        var property = propertyBuilder.Metadata;

        var clrType = Nullable.GetUnderlyingType(property.ClrType) ?? property.ClrType;
        // Apply to JsonElement properties regardless of whether column type is set
        // This ensures direct JsonElement properties use BigQueryJsonTypeMapping, not BigQueryOwnedJsonTypeMapping
        if (clrType == typeof(JsonElement))
        {
            property.SetTypeMapping(_jsonTypeMapping ??= new BigQueryJsonTypeMapping("JSON", typeof(JsonElement)));
        }
    }
}
