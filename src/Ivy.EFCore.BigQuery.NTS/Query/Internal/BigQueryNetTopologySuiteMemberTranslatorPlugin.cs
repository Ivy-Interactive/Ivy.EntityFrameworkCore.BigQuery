using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;

namespace Ivy.EntityFrameworkCore.BigQuery.NetTopologySuite.Query.Internal;

/// <summary>
/// Plugin for registering NetTopologySuite member translators.
/// </summary>
public class BigQueryNetTopologySuiteMemberTranslatorPlugin : IMemberTranslatorPlugin
{
    /// <summary>
    /// Creates a new instance.
    /// </summary>
    public BigQueryNetTopologySuiteMemberTranslatorPlugin(
        ISqlExpressionFactory sqlExpressionFactory,
        IRelationalTypeMappingSource typeMappingSource)
    {
        Translators = new IMemberTranslator[]
        {
            new BigQueryGeographyMemberTranslator(sqlExpressionFactory, typeMappingSource)
        };
    }

    /// <inheritdoc />
    public virtual IEnumerable<IMemberTranslator> Translators { get; }
}
