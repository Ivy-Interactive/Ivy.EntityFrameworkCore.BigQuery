using Ivy.EntityFrameworkCore.BigQuery.Storage.Internal.Mapping;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
using System.Linq.Expressions;
using System.Reflection;

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal;

public class BigQuerySqlTranslatingExpressionVisitor : RelationalSqlTranslatingExpressionVisitor
{
    private static readonly MethodInfo StringJoinMethodInfo
        = typeof(string).GetRuntimeMethod(nameof(string.Join), [typeof(string), typeof(string[])])!;

    private readonly ISqlExpressionFactory _sqlExpressionFactory;
    private readonly IRelationalTypeMappingSource _typeMappingSource;

    public BigQuerySqlTranslatingExpressionVisitor(
        RelationalSqlTranslatingExpressionVisitorDependencies dependencies,
        RelationalQueryCompilationContext queryCompilationContext,
        QueryableMethodTranslatingExpressionVisitor queryableMethodTranslatingExpressionVisitor)
        : base(dependencies, queryCompilationContext, queryableMethodTranslatingExpressionVisitor)
    {
        _sqlExpressionFactory = dependencies.SqlExpressionFactory;
        _typeMappingSource = dependencies.TypeMappingSource;
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
    /// Handles binary operations that need special translation for BigQuery.
    /// </summary>
    protected override Expression VisitBinary(BinaryExpression binaryExpression)
    {
        if (binaryExpression.NodeType == ExpressionType.Subtract)
        {
            // TimeOnly - TimeOnly -> TIME_DIFF(left, right, MICROSECOND) returning TimeSpan
            if (binaryExpression.Left.Type == typeof(TimeOnly)
                && binaryExpression.Right.Type == typeof(TimeOnly))
            {
                var left = Visit(binaryExpression.Left);
                var right = Visit(binaryExpression.Right);

                if (left is SqlExpression sqlLeft && right is SqlExpression sqlRight)
                {
                    var timeSpanMapping = _typeMappingSource.FindMapping(typeof(TimeSpan));

                    // TIME_DIFF(time1, time2, MICROSECOND) returns INT64 microseconds
                    return _sqlExpressionFactory.Function(
                        "TIME_DIFF",
                        [sqlLeft, sqlRight, _sqlExpressionFactory.Fragment("MICROSECOND")],
                        nullable: true,
                        argumentsPropagateNullability: [true, true, false],
                        typeof(TimeSpan),
                        timeSpanMapping);
                }
            }

            // DateTime - DateTime -> DATETIME_DIFF(left, right, MICROSECOND) returning TimeSpan
            if (binaryExpression.Left.Type == typeof(DateTime)
                && binaryExpression.Right.Type == typeof(DateTime))
            {
                var left = Visit(binaryExpression.Left);
                var right = Visit(binaryExpression.Right);

                if (left is SqlExpression sqlLeft && right is SqlExpression sqlRight)
                {
                    var timeSpanMapping = _typeMappingSource.FindMapping(typeof(TimeSpan));

                    // DATETIME_DIFF(datetime1, datetime2, MICROSECOND) returns INT64 microseconds
                    return _sqlExpressionFactory.Function(
                        "DATETIME_DIFF",
                        [sqlLeft, sqlRight, _sqlExpressionFactory.Fragment("MICROSECOND")],
                        nullable: true,
                        argumentsPropagateNullability: [true, true, false],
                        typeof(TimeSpan),
                        timeSpanMapping);
                }
            }

            // DateTimeOffset - DateTimeOffset -> TIMESTAMP_DIFF(left, right, MICROSECOND) returning TimeSpan
            if (binaryExpression.Left.Type == typeof(DateTimeOffset)
                && binaryExpression.Right.Type == typeof(DateTimeOffset))
            {
                var left = Visit(binaryExpression.Left);
                var right = Visit(binaryExpression.Right);

                if (left is SqlExpression sqlLeft && right is SqlExpression sqlRight)
                {
                    var timeSpanMapping = _typeMappingSource.FindMapping(typeof(TimeSpan));

                    // TIMESTAMP_DIFF(timestamp1, timestamp2, MICROSECOND) returns INT64 microseconds
                    return _sqlExpressionFactory.Function(
                        "TIMESTAMP_DIFF",
                        [sqlLeft, sqlRight, _sqlExpressionFactory.Fragment("MICROSECOND")],
                        nullable: true,
                        argumentsPropagateNullability: [true, true, false],
                        typeof(TimeSpan),
                        timeSpanMapping);
                }
            }

            // DateTimeOffset - TimeSpan -> TIMESTAMP_SUB(timestamp, INTERVAL value MICROSECOND)
            if (binaryExpression.Left.Type == typeof(DateTimeOffset)
                && binaryExpression.Right.Type == typeof(TimeSpan))
            {
                var left = Visit(binaryExpression.Left);
                var right = Visit(binaryExpression.Right);

                if (left is SqlExpression sqlLeft && right is SqlExpression sqlRight)
                {
                    var timestampMapping = _typeMappingSource.FindMapping(typeof(DateTimeOffset));
                    var timeSpanMapping = _typeMappingSource.FindMapping(typeof(TimeSpan));

                    if (sqlRight.TypeMapping == null)
                    {
                        sqlRight = _sqlExpressionFactory.ApplyTypeMapping(sqlRight, timeSpanMapping);
                    }

                    // TIMESTAMP_SUB(timestamp, INTERVAL value MICROSECOND)
                    var intervalExpression = new BigQueryIntervalExpression(
                        sqlRight,
                        "MICROSECOND",
                        _typeMappingSource.FindMapping(typeof(long)));

                    return _sqlExpressionFactory.Function(
                        "TIMESTAMP_SUB",
                        [sqlLeft, intervalExpression],
                        nullable: true,
                        argumentsPropagateNullability: [true, true],
                        typeof(DateTimeOffset),
                        timestampMapping);
                }
            }
        }

        return base.VisitBinary(binaryExpression);
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
    /// Translates non-aggregate string.Join to ARRAY_TO_STRING.
    /// </summary>
    protected override Expression VisitMethodCall(MethodCallExpression methodCallExpression)
    {
        var method = methodCallExpression.Method;
        
        if (method == StringJoinMethodInfo
            && methodCallExpression.Arguments[1] is NewArrayExpression newArrayExpression)
        {
            if (Visit(methodCallExpression.Arguments[0]) is not SqlExpression delimiter)
            {
                return QueryCompilationContext.NotTranslatedExpression;
            }

            var arrayElements = new SqlExpression[newArrayExpression.Expressions.Count];

            for (var i = 0; i < newArrayExpression.Expressions.Count; i++)
            {
                var argument = newArrayExpression.Expressions[i];
                if (Visit(argument) is not SqlExpression sqlArgument)
                {
                    return QueryCompilationContext.NotTranslatedExpression;
                }
                
                arrayElements[i] = _sqlExpressionFactory.Coalesce(
                    sqlArgument,
                    _sqlExpressionFactory.Constant(string.Empty));
            }

            // ARRAY_TO_STRING(array_expression, delimiter[, null_text])
            var elementTypeMapping = ExpressionExtensions.InferTypeMapping(arrayElements)
                ?? _typeMappingSource.FindMapping(typeof(string));
            var arrayTypeMapping = _typeMappingSource.FindMapping(typeof(string[]));
            var arrayExpression = new BigQueryInlineArrayExpression(arrayElements, typeof(string[]), arrayTypeMapping);

            return _sqlExpressionFactory.Function(
                "ARRAY_TO_STRING",
                [arrayExpression, delimiter, _sqlExpressionFactory.Constant(string.Empty)],
                nullable: false,
                argumentsPropagateNullability: [false, true, false],
                typeof(string));
        }

        return base.VisitMethodCall(methodCallExpression);
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
