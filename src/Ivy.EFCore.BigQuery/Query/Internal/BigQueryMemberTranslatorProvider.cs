using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal;

public class BigQueryMemberTranslatorProvider : RelationalMemberTranslatorProvider
{
    public virtual BigQueryJsonPocoTranslator JsonPocoTranslator { get; }

    public BigQueryMemberTranslatorProvider(
        RelationalMemberTranslatorProviderDependencies dependencies,
        IRelationalTypeMappingSource typeMappingSource,
        IModel model)
        : base(dependencies)
    {
        var sqlExpressionFactory = (BigQuerySqlExpressionFactory)dependencies.SqlExpressionFactory;
        JsonPocoTranslator = new BigQueryJsonPocoTranslator(typeMappingSource, sqlExpressionFactory, model);

        AddTranslators(
        [
            new BigQueryStringMemberTranslator(dependencies.SqlExpressionFactory),
            new BigQueryStructMemberTranslator(dependencies.SqlExpressionFactory),
            new BigQueryArrayMemberTranslator(dependencies.SqlExpressionFactory),
            new BigQueryDateTimeMemberTranslator(dependencies.SqlExpressionFactory, typeMappingSource),
            new BigQueryJsonDomTranslator(typeMappingSource, sqlExpressionFactory, model),
            JsonPocoTranslator
        ]);
    }
}