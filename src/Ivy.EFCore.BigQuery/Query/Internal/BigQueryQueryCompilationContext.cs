using Microsoft.EntityFrameworkCore.Query;

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal
{
    public class BigQueryQueryCompilationContext : RelationalQueryCompilationContext
    {
        public BigQueryQueryCompilationContext(
            QueryCompilationContextDependencies dependencies,
            RelationalQueryCompilationContextDependencies relationalDependencies,
            bool async)
            : base(dependencies, relationalDependencies, async)
        {
        }

        public override Microsoft.EntityFrameworkCore.Metadata.IModel Model
        {
            get
            {
                var model = base.Model;
                return model;
            }
        }

        public override bool IsBuffering
            => base.IsBuffering || QuerySplittingBehavior == Microsoft.EntityFrameworkCore.QuerySplittingBehavior.SplitQuery;

        public override bool SupportsPrecompiledQuery
            => true;
    }
}