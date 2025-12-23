using Microsoft.EntityFrameworkCore.Query;

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal;

public class BigQueryRelationalParameterBasedSqlProcessorFactory : IRelationalParameterBasedSqlProcessorFactory
{
    private readonly RelationalParameterBasedSqlProcessorDependencies _dependencies;

    public BigQueryRelationalParameterBasedSqlProcessorFactory(
        RelationalParameterBasedSqlProcessorDependencies dependencies)
    {
        _dependencies = dependencies;
    }

    public virtual RelationalParameterBasedSqlProcessor Create(RelationalParameterBasedSqlProcessorParameters parameters)
    {
        return new BigQueryParameterBasedSqlProcessor(_dependencies, parameters);
    }
}
