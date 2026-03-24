using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore.Metadata.Conventions;

namespace Ivy.EntityFrameworkCore.BigQuery.Metadata.Conventions;

/// <summary>
/// Convention that sets the JSON column type to "JSON" for all entity types mapped to JSON.
/// EF Core 10 requires providers to specify the JSON column store type.
/// </summary>
public class BigQueryJsonColumnTypeConvention : IModelFinalizingConvention
{
    private const string JsonColumnType = "JSON";

    /// <inheritdoc />
    public void ProcessModelFinalizing(
        IConventionModelBuilder modelBuilder,
        IConventionContext<IConventionModelBuilder> context)
    {
        foreach (var entityType in modelBuilder.Metadata.GetEntityTypes())
        {
            if (entityType.IsMappedToJson())
            {
                if (entityType.GetContainerColumnType() == null)
                {
                    entityType.SetContainerColumnType(JsonColumnType);
                }
            }
        }
    }
}