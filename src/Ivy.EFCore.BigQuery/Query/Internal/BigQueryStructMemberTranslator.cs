using Ivy.EntityFrameworkCore.BigQuery.Storage.Internal.Mapping;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using System.Reflection;

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal;

/// <summary>
/// Provides translation services for STRUCT member access to BigQuery SQL field access.
/// Translates STRUCT property access to STRUCT.field syntax.
/// Handles member access on properties with BigQueryStructTypeMapping.
/// </summary>
public class BigQueryStructMemberTranslator : IMemberTranslator
{
    private readonly ISqlExpressionFactory _sqlExpressionFactory;

    public BigQueryStructMemberTranslator(ISqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MemberInfo member,
        Type returnType,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (instance?.TypeMapping is BigQueryStructTypeMapping structTypeMapping)
        {
            if (member is PropertyInfo property)
            {
                var fieldName = property.Name;

                var field = structTypeMapping.Fields.FirstOrDefault(f => f.Name == fieldName);
                if (field != null)
                {
                    return new BigQueryStructAccessExpression(
                        instance,
                        fieldName,
                        field.Type,
                        field.TypeMapping);
                }
            }
        }

        return null;
    }
}
