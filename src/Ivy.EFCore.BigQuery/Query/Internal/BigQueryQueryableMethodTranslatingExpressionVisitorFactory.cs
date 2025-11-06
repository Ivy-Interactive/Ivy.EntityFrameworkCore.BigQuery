using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal
{
    public class BigQueryQueryableMethodTranslatingExpressionVisitorFactory : IQueryableMethodTranslatingExpressionVisitorFactory
    {
        public BigQueryQueryableMethodTranslatingExpressionVisitorFactory(
            QueryableMethodTranslatingExpressionVisitorDependencies dependencies,
            RelationalQueryableMethodTranslatingExpressionVisitorDependencies relationalDependencies)
        {
            Dependencies = dependencies;
            RelationalDependencies = relationalDependencies;
        }

        protected virtual QueryableMethodTranslatingExpressionVisitorDependencies Dependencies { get; }
        public RelationalQueryableMethodTranslatingExpressionVisitorDependencies RelationalDependencies { get; }

        public virtual QueryableMethodTranslatingExpressionVisitor Create(QueryCompilationContext queryCompilationContext)
            => new BigQueryQueryableMethodTranslatingExpressionVisitor(
           Dependencies,
           RelationalDependencies,
           (RelationalQueryCompilationContext)queryCompilationContext);
    }
}