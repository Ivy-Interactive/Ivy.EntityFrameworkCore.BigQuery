
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
using System.Diagnostics.CodeAnalysis;

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal;

public class BigQuerySqlExpressionFactory : SqlExpressionFactory
{
    private readonly IRelationalTypeMappingSource _typeMappingSource;

    public BigQuerySqlExpressionFactory(SqlExpressionFactoryDependencies dependencies) : base(dependencies)
    {
        _typeMappingSource = dependencies.TypeMappingSource;
    }



    public override SqlExpression Convert(SqlExpression operand, Type type, RelationalTypeMapping? typeMapping)
    {
        if (UnwrapNullableType(type) == typeof(DateTime) && UnwrapNullableType(operand.Type) == typeof(DateTimeOffset))
        {
            return new SqlFunctionExpression(
                "DATETIME",
                new[] { operand },
                nullable: true,
                argumentsPropagateNullability: new[] { true },
                type,
                typeMapping);
        }

        return base.Convert(operand, type, typeMapping);
    }

    public static Type UnwrapNullableType(Type type)
    => Nullable.GetUnderlyingType(type) ?? type;


    //[return: NotNullIfNotNull("sqlExpression")]
    //public override SqlExpression? ApplyTypeMapping(SqlExpression? sqlExpression, RelationalTypeMapping? typeMapping)
    //{
    //    if (sqlExpression is not null && sqlExpression.TypeMapping is null)
    //    {
    //        sqlExpression = sqlExpression switch
    //        {
    //            _ => base.ApplyTypeMapping(sqlExpression, typeMapping)
    //        };
    //    }

    //    return sqlExpression;
    //}
}
