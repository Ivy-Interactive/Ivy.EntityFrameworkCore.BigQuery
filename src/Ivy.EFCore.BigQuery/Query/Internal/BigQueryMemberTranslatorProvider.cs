using Microsoft.EntityFrameworkCore.Query;

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal;

public class BigQueryMemberTranslatorProvider : RelationalMemberTranslatorProvider
{
    public BigQueryMemberTranslatorProvider(RelationalMemberTranslatorProviderDependencies dependencies)
        : base(dependencies)
    {
        AddTranslators(
        [
            new BigQueryStringMemberTranslator(dependencies.SqlExpressionFactory)
        ]);
    }
}