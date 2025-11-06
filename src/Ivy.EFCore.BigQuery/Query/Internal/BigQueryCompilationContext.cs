using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal
{
    public class BigQueryCompilationContext(QueryCompilationContextDependencies dependencies, bool async)
    : QueryCompilationContext(dependencies, async)
    {
        public virtual IEntityType? RootEntityType { get; internal set; }
    }
}
