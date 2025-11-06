using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query;

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal
{
    public class BigQueryQueryCompilationContextFactory : IQueryCompilationContextFactory
    {
        private readonly QueryCompilationContextDependencies _dependencies;
        private readonly RelationalQueryCompilationContextDependencies _relationalDependencies;

        public BigQueryQueryCompilationContextFactory(
            QueryCompilationContextDependencies dependencies,
            RelationalQueryCompilationContextDependencies relationalDependencies)          
        {
            _dependencies = dependencies;
            _relationalDependencies = relationalDependencies;
        }              

        public virtual QueryCompilationContext Create(bool async)
            => new BigQueryQueryCompilationContext(_dependencies, _relationalDependencies, async);
    }
}