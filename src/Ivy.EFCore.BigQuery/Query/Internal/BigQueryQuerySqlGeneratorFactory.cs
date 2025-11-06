using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal
{
    public class BigQueryQuerySqlGeneratorFactory : IQuerySqlGeneratorFactory
    {
        private readonly QuerySqlGeneratorDependencies _dependencies;
        private readonly IRelationalTypeMappingSource _typeMappingSource;

        public BigQueryQuerySqlGeneratorFactory(
            QuerySqlGeneratorDependencies dependencies,
            IRelationalTypeMappingSource typeMappingSource)
        {
            _dependencies = dependencies;
            _typeMappingSource = typeMappingSource;
        }

        public virtual QuerySqlGenerator Create()
            => new BigQueryQuerySqlGenerator(
                _dependencies,
                _typeMappingSource);
    }
}
