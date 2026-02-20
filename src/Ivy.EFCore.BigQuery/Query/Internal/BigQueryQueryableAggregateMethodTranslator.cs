using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
using System.Reflection;

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal;

/// <summary>
/// Translates LINQ aggregate methods (Count, Sum, Average, Min, Max) to BigQuery-specific SQL.
/// Handles type conversions where BigQuery return types differ from .NET expectations.
/// </summary>
public class BigQueryQueryableAggregateMethodTranslator : IAggregateMethodCallTranslator
{
    private static readonly bool[] FalseArrays1 = [false];

    private readonly BigQuerySqlExpressionFactory _sqlExpressionFactory;
    private readonly IRelationalTypeMappingSource _typeMappingSource;

    public BigQueryQueryableAggregateMethodTranslator(
        BigQuerySqlExpressionFactory sqlExpressionFactory,
        IRelationalTypeMappingSource typeMappingSource)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
        _typeMappingSource = typeMappingSource;
    }

    public virtual SqlExpression? Translate(
        MethodInfo method,
        EnumerableExpression source,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (method.DeclaringType != typeof(Queryable))
        {
            return null;
        }

        var methodInfo = method.IsGenericMethod
            ? method.GetGenericMethodDefinition()
            : method;

        switch (methodInfo.Name)
        {
            // BigQuery COUNT() returns INT64, but .NET Count() expects int
            // Need to convert from long to int
            case nameof(Queryable.Count)
                when methodInfo == QueryableMethods.CountWithoutPredicate
                || methodInfo == QueryableMethods.CountWithPredicate:
                var countSqlExpression = (source.Selector as SqlExpression) ?? _sqlExpressionFactory.Fragment("*");
                return _sqlExpressionFactory.Convert(
                    _sqlExpressionFactory.Function(
                        "COUNT",
                        [countSqlExpression],
                        nullable: false,
                        argumentsPropagateNullability: FalseArrays1,
                        typeof(long)),
                    typeof(int),
                    _typeMappingSource.FindMapping(typeof(int)));

            // BigQuery COUNT() returns INT64, which matches .NET long
            // No conversion needed
            case nameof(Queryable.LongCount)
                when methodInfo == QueryableMethods.LongCountWithoutPredicate
                || methodInfo == QueryableMethods.LongCountWithPredicate:
                var longCountSqlExpression = (source.Selector as SqlExpression) ?? _sqlExpressionFactory.Fragment("*");
                return _sqlExpressionFactory.Function(
                    "COUNT",
                    [longCountSqlExpression],
                    nullable: false,
                    argumentsPropagateNullability: FalseArrays1,
                    typeof(long));

            // BigQuery AVG behavior:
            // - INT64 input -> FLOAT64 output (but with precision loss!)
            // - FLOAT64 input -> FLOAT64 output
            // - NUMERIC/BIGNUMERIC input -> same type output (exact)
            //
            // To avoid precision loss for integer types, we cast to NUMERIC first,
            // compute AVG (which returns exact NUMERIC), then cast back to FLOAT64.
            // This ensures exact computation while returning the expected double type.
            case nameof(Queryable.Average)
                when (QueryableMethods.IsAverageWithoutSelector(methodInfo)
                    || QueryableMethods.IsAverageWithSelector(methodInfo))
                && source.Selector is SqlExpression averageSqlExpression:
                var averageInputType = averageSqlExpression.Type;

                // For int and long, cast to NUMERIC first for exact computation,
                // then cast result back to FLOAT64 to match .NET's double expectation
                if (averageInputType == typeof(int) || averageInputType == typeof(long))
                {
                    // Cast input to NUMERIC for exact arithmetic
                    var numericInput = _sqlExpressionFactory.Convert(
                        averageSqlExpression,
                        typeof(decimal),
                        _typeMappingSource.FindMapping(typeof(decimal)));

                    // AVG on NUMERIC returns NUMERIC (exact)
                    var avgNumeric = _sqlExpressionFactory.Function(
                        "AVG",
                        [numericInput],
                        nullable: true,
                        argumentsPropagateNullability: FalseArrays1,
                        typeof(decimal),
                        _typeMappingSource.FindMapping(typeof(decimal)));

                    // Cast back to FLOAT64 to match expected return type
                    return _sqlExpressionFactory.Convert(
                        avgNumeric,
                        typeof(double),
                        _typeMappingSource.FindMapping(typeof(double)));
                }

                // For float, need to ensure we return float (BigQuery returns FLOAT64)
                if (averageInputType == typeof(float))
                {
                    return _sqlExpressionFactory.Convert(
                        _sqlExpressionFactory.Function(
                            "AVG",
                            [averageSqlExpression],
                            nullable: true,
                            argumentsPropagateNullability: FalseArrays1,
                            typeof(double)),
                        typeof(float),
                        averageSqlExpression.TypeMapping);
                }

                // For decimal, NUMERIC, BIGNUMERIC - preserve type (already exact)
                return _sqlExpressionFactory.Function(
                    "AVG",
                    [averageSqlExpression],
                    nullable: true,
                    argumentsPropagateNullability: FalseArrays1,
                    averageSqlExpression.Type,
                    averageSqlExpression.TypeMapping);

            // BigQuery SUM returns same type as input
            // INT64 → INT64, FLOAT64 → FLOAT64, NUMERIC → NUMERIC, etc.
            case nameof(Queryable.Sum)
                when (QueryableMethods.IsSumWithoutSelector(methodInfo)
                    || QueryableMethods.IsSumWithSelector(methodInfo))
                && source.Selector is SqlExpression sumSqlExpression:
                return _sqlExpressionFactory.Function(
                    "SUM",
                    [sumSqlExpression],
                    nullable: true,
                    argumentsPropagateNullability: FalseArrays1,
                    sumSqlExpression.Type,
                    sumSqlExpression.TypeMapping);

            // BigQuery MIN/MAX preserve input type
            case nameof(Queryable.Min)
                when (methodInfo == QueryableMethods.MinWithoutSelector
                    || methodInfo == QueryableMethods.MinWithSelector)
                && source.Selector is SqlExpression minSqlExpression:
                return _sqlExpressionFactory.Function(
                    "MIN",
                    [minSqlExpression],
                    nullable: true,
                    argumentsPropagateNullability: FalseArrays1,
                    minSqlExpression.Type,
                    minSqlExpression.TypeMapping);

            case nameof(Queryable.Max)
                when (methodInfo == QueryableMethods.MaxWithoutSelector
                    || methodInfo == QueryableMethods.MaxWithSelector)
                && source.Selector is SqlExpression maxSqlExpression:
                return _sqlExpressionFactory.Function(
                    "MAX",
                    [maxSqlExpression],
                    nullable: true,
                    argumentsPropagateNullability: FalseArrays1,
                    maxSqlExpression.Type,
                    maxSqlExpression.TypeMapping);
        }

        return null;
    }
}
