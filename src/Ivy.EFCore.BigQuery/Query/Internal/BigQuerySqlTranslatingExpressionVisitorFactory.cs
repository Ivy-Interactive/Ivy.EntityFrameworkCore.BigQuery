using Microsoft.EntityFrameworkCore.Query;

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal;

public class BigQuerySqlTranslatingExpressionVisitorFactory : IRelationalSqlTranslatingExpressionVisitorFactory
{
    private readonly RelationalSqlTranslatingExpressionVisitorDependencies _dependencies;

    public BigQuerySqlTranslatingExpressionVisitorFactory(
        RelationalSqlTranslatingExpressionVisitorDependencies dependencies)
    {
        _dependencies = dependencies;
    }

    public virtual RelationalSqlTranslatingExpressionVisitor Create(
        QueryCompilationContext queryCompilationContext,
        QueryableMethodTranslatingExpressionVisitor queryableMethodTranslatingExpressionVisitor)
    {
        return new BigQuerySqlTranslatingExpressionVisitor(
            _dependencies,
            (RelationalQueryCompilationContext)queryCompilationContext,
            queryableMethodTranslatingExpressionVisitor);
    }
}