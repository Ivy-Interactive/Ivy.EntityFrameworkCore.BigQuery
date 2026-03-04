using Ivy.EntityFrameworkCore.BigQuery.Storage.Internal.Mapping;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using System.Linq.Expressions;
using System.Reflection;

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal;

public class BigQuerySqlTranslatingExpressionVisitor : RelationalSqlTranslatingExpressionVisitor
{
    private readonly ISqlExpressionFactory _sqlExpressionFactory;

    public BigQuerySqlTranslatingExpressionVisitor(
        RelationalSqlTranslatingExpressionVisitorDependencies dependencies,
        RelationalQueryCompilationContext queryCompilationContext,
        QueryableMethodTranslatingExpressionVisitor queryableMethodTranslatingExpressionVisitor)
        : base(dependencies, queryCompilationContext, queryableMethodTranslatingExpressionVisitor)
    {
        _sqlExpressionFactory = dependencies.SqlExpressionFactory;
    }

    protected override Expression VisitExtension(Expression extensionExpression)
    {
        return extensionExpression switch
        {
            BigQueryStructAccessExpression or BigQueryStructConstructorExpression => extensionExpression,
            _ => base.VisitExtension(extensionExpression),
        };
    }

    /// <summary>
    /// Translates byte[].Length to BigQuery's LENGTH function.
    /// </summary>
    protected override Expression VisitUnary(UnaryExpression unaryExpression)
    {
        if (unaryExpression.NodeType == ExpressionType.ArrayLength
            && Visit(unaryExpression.Operand) is SqlExpression sqlOperand)
        {
            // Translate Length on byte[] (BYTES in BigQuery)
            if (sqlOperand.Type == typeof(byte[])
                && sqlOperand.TypeMapping is BigQueryByteArrayTypeMapping or null)
            {
                return _sqlExpressionFactory.Function(
                    "LENGTH",
                    [sqlOperand],
                    nullable: true,
                    argumentsPropagateNullability: [true],
                    typeof(int));
            }
        }

        return base.VisitUnary(unaryExpression);
    }

    /// STRUCT field access. e.ContactInfo.City -> e.contact_info.City
    protected override Expression VisitMember(MemberExpression memberExpression)
    {
        var innerExpression = Visit(memberExpression.Expression);

        if (innerExpression is SqlExpression sqlExpression &&
            sqlExpression.TypeMapping is BigQueryStructTypeMapping structTypeMapping)
        {
            if (memberExpression.Member is PropertyInfo propertyInfo)
            {
                var fieldName = propertyInfo.Name;
                var field = structTypeMapping.Fields.FirstOrDefault(f =>
                    string.Equals(f.PropertyName ?? f.Name, propertyInfo.Name, StringComparison.OrdinalIgnoreCase));

                if (field != null)
                {
                    return new BigQueryStructAccessExpression(
                        sqlExpression,
                        field.Name,
                        field.Type,
                        field.TypeMapping);
                }
            }
        }

        return base.VisitMember(memberExpression);
    }

    /// <summary>
    /// Translates Math.Max to BigQuery's GREATEST function.
    /// </summary>
    public override SqlExpression? GenerateGreatest(IReadOnlyList<SqlExpression> expressions, Type resultType)
    {
        var resultTypeMapping = ExpressionExtensions.InferTypeMapping(expressions);

        return _sqlExpressionFactory.Function(
            "GREATEST",
            expressions,
            nullable: true,
            argumentsPropagateNullability: Enumerable.Repeat(false, expressions.Count),
            resultType,
            resultTypeMapping);
    }

    /// <summary>
    /// Translates Math.Min to BigQuery's LEAST function.
    /// </summary>
    public override SqlExpression? GenerateLeast(IReadOnlyList<SqlExpression> expressions, Type resultType)
    {
        var resultTypeMapping = ExpressionExtensions.InferTypeMapping(expressions);

        return _sqlExpressionFactory.Function(
            "LEAST",
            expressions,
            nullable: true,
            argumentsPropagateNullability: Enumerable.Repeat(false, expressions.Count),
            resultType,
            resultTypeMapping);
    }
}
