
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

    /// <summary>
    /// STRUCT field access expression: struct.field
    /// </summary>
    public virtual BigQueryStructAccessExpression StructAccess(
        SqlExpression instance,
        string fieldName,
        Type type,
        RelationalTypeMapping? typeMapping)
    {
        return new BigQueryStructAccessExpression(instance, fieldName, type, typeMapping);
    }

    /// <summary>
    /// STRUCT constructor expression: STRUCT&lt;...&gt;(...) or STRUCT(... AS ...)
    /// </summary>
    public virtual BigQueryStructConstructorExpression StructConstructor(
        IReadOnlyList<SqlExpression> arguments,
        IReadOnlyList<string> fieldNames,
        IReadOnlyList<RelationalTypeMapping?> fieldTypeMappings,
        Type type,
        RelationalTypeMapping? typeMapping,
        string? explicitType = null)
    {
        return new BigQueryStructConstructorExpression(
            arguments,
            fieldNames,
            fieldTypeMappings,
            type,
            typeMapping,
            explicitType);
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
