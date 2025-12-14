using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore.Metadata.Conventions;
using Microsoft.EntityFrameworkCore.Storage;

namespace Ivy.EntityFrameworkCore.BigQuery.Metadata.Conventions;

/// <summary>
/// Convention to configure properties of types marked with [BigQueryStruct] as value properties
/// rather than owned/complex types.
/// </summary>
public class BigQueryStructPropertyConvention :
    IComplexPropertyAddedConvention,
    IPropertyAddedConvention
{
    private readonly IRelationalTypeMappingSource _typeMappingSource;

    public BigQueryStructPropertyConvention(IRelationalTypeMappingSource typeMappingSource)
    {
        _typeMappingSource = typeMappingSource;
    }

    public void ProcessPropertyAdded(
        IConventionPropertyBuilder propertyBuilder,
        IConventionContext<IConventionPropertyBuilder> context)
    {
        var property = propertyBuilder.Metadata;
        var propertyType = property.ClrType;

        if (HasBigQueryStructAttribute(propertyType))
        {
            var typeMapping = _typeMappingSource.FindMapping(propertyType);
            if (typeMapping != null)
            {
                property.SetTypeMapping(typeMapping);
            }
        }
    }

    public void ProcessComplexPropertyAdded(
        IConventionComplexPropertyBuilder propertyBuilder,
        IConventionContext<IConventionComplexPropertyBuilder> context)
    {
        var property = propertyBuilder.Metadata;
        var propertyType = property.ClrType;

        if (HasBigQueryStructAttribute(propertyType))
        {
            property.DeclaringType.RemoveComplexProperty(property.Name);

            var entityType = property.DeclaringType as Microsoft.EntityFrameworkCore.Metadata.IConventionEntityType;
            if (entityType != null)
            {
                var prop = entityType.AddProperty(property.Name, propertyType);
                if (prop != null)
                {
                    var typeMapping = _typeMappingSource.FindMapping(propertyType);
                    if (typeMapping != null)
                    {
                        prop.SetTypeMapping(typeMapping);
                        prop.SetValueConverter(typeMapping.Converter);
                        prop.SetValueComparer(typeMapping.Comparer);
                    }
                }
            }
        }
    }

    private static bool HasBigQueryStructAttribute(Type type)
    {
        var attributes = type.GetCustomAttributes(typeof(BigQueryStructAttribute), inherit: true);
        return attributes.Length > 0;
    }
}
