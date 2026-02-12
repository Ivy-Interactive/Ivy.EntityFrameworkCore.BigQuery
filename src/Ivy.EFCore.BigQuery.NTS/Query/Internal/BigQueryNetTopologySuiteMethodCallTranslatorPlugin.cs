using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;

namespace Ivy.EntityFrameworkCore.BigQuery.NetTopologySuite.Query.Internal;

/// <summary>
/// Plugin for registering NetTopologySuite method call translators.
/// </summary>
public class BigQueryNetTopologySuiteMethodCallTranslatorPlugin : IMethodCallTranslatorPlugin
{
    /// <summary>
    /// Creates a new instance.
    /// </summary>
    public BigQueryNetTopologySuiteMethodCallTranslatorPlugin(
        ISqlExpressionFactory sqlExpressionFactory,
        IRelationalTypeMappingSource typeMappingSource)
    {
        Translators = new IMethodCallTranslator[]
        {
            new BigQueryGeographyMethodTranslator(sqlExpressionFactory, typeMappingSource)
        };
    }

    /// <inheritdoc />
    public virtual IEnumerable<IMethodCallTranslator> Translators { get; }
}