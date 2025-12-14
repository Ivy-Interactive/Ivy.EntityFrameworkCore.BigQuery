using Ivy.EntityFrameworkCore.BigQuery.Storage.Internal.Mapping;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using System.Linq.Expressions;
using System.Reflection;

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal;

public class BigQuerySqlTranslatingExpressionVisitor : RelationalSqlTranslatingExpressionVisitor
{
    public BigQuerySqlTranslatingExpressionVisitor(
        RelationalSqlTranslatingExpressionVisitorDependencies dependencies,
        RelationalQueryCompilationContext queryCompilationContext,
        QueryableMethodTranslatingExpressionVisitor queryableMethodTranslatingExpressionVisitor)
        : base(dependencies, queryCompilationContext, queryableMethodTranslatingExpressionVisitor)
    {
    }

    protected override Expression VisitExtension(Expression extensionExpression)
    {
        return extensionExpression switch
        {
            BigQueryStructAccessExpression or BigQueryStructConstructorExpression => extensionExpression,
            _ => base.VisitExtension(extensionExpression),
        };
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
}
