using Microsoft.EntityFrameworkCore.Query;

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal;

public class BigQueryMethodCallTranslatorProvider : RelationalMethodCallTranslatorProvider
{
    public BigQueryMethodCallTranslatorProvider(RelationalMethodCallTranslatorProviderDependencies dependencies)
        : base(dependencies)
    {
        var sqlExpressionFactory = (BigQuerySqlExpressionFactory)dependencies.SqlExpressionFactory;
        var typeMappingSource = dependencies.RelationalTypeMappingSource;

        AddTranslators(
        [
            new BigQueryStringMethodTranslator(dependencies.SqlExpressionFactory),
            new BigQueryMathMethodTranslator(dependencies.SqlExpressionFactory, typeMappingSource),
            new BigQueryArrayMethodTranslator(sqlExpressionFactory),
        ]);
    }
}