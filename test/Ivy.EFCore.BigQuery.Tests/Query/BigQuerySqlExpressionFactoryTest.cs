
using Ivy.EntityFrameworkCore.BigQuery.Query.Internal;
using Ivy.EntityFrameworkCore.BigQuery.Storage.Internal;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.Json;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using Xunit;

namespace Ivy.EntityFrameworkCore.BigQuery.Tests.Query
{
    public class BigQuerySqlExpressionFactoryTest
    {
        private readonly BigQuerySqlExpressionFactory _factory;
        private readonly IRelationalTypeMappingSource _typeMappingSource;

        public BigQuerySqlExpressionFactoryTest()
        {
            _typeMappingSource = new BigQueryTypeMappingSource(
                new TypeMappingSourceDependencies(
                        new ValueConverterSelector(new ValueConverterSelectorDependencies()),
                        new JsonValueReaderWriterSource(new JsonValueReaderWriterSourceDependencies()),
                        []
                    ),
                    new RelationalTypeMappingSourceDependencies([]));

            var model = new Model();

            var dependencies = new SqlExpressionFactoryDependencies(
                model,
                _typeMappingSource
                );

            _factory = new BigQuerySqlExpressionFactory(dependencies);
        }

        [Fact]
        public void Convert_DateTimeOffset_to_DateTime_translates_to_DATETIME_function()
        {
            var initialExpression = new SqlParameterExpression("p0", typeof(DateTimeOffset), _typeMappingSource.FindMapping(typeof(DateTimeOffset)));

            var result = _factory.Convert(initialExpression, typeof(DateTime), _typeMappingSource.FindMapping(typeof(DateTime)));

            var sqlFunctionExpression = Assert.IsType<SqlFunctionExpression>(result);
            Assert.Equal("DATETIME", sqlFunctionExpression.Name);
            Assert.Equal(typeof(DateTime), sqlFunctionExpression.Type);
            Assert.Collection(sqlFunctionExpression.Arguments,
                arg => Assert.Same(initialExpression, arg));
        }

        [Fact]
        public void ApplyTypeMapping_applies_mapping_to_expression_without_one()
        {
            var initialExpression = new SqlConstantExpression(1, null);
            var typeMapping = _typeMappingSource.FindMapping(typeof(int));

            var result = _factory.ApplyTypeMapping(initialExpression, typeMapping);

            Assert.NotNull(result);
            Assert.Same(typeMapping, result.TypeMapping);
        }
    }
}
