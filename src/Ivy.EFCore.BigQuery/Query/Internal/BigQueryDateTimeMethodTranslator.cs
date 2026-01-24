using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal;

/// <summary>
/// Translates DateTime, DateTimeOffset, DateOnly, and TimeOnly method calls to BigQuery SQL.
/// </summary>
public class BigQueryDateTimeMethodTranslator : IMethodCallTranslator
{
    private readonly ISqlExpressionFactory _sqlExpressionFactory;
    private readonly IRelationalTypeMappingSource _typeMappingSource;

    private static readonly bool[] TrueArrays1 = [true];
    private static readonly bool[] TrueArrays2 = [true, true];
    private static readonly bool[] TrueArrays3 = [true, true, true];

    // DateTime Add
    private static readonly MethodInfo DateTime_AddYears = typeof(DateTime).GetRuntimeMethod(nameof(DateTime.AddYears), [typeof(int)])!;
    private static readonly MethodInfo DateTime_AddMonths = typeof(DateTime).GetRuntimeMethod(nameof(DateTime.AddMonths), [typeof(int)])!;
    private static readonly MethodInfo DateTime_AddDays = typeof(DateTime).GetRuntimeMethod(nameof(DateTime.AddDays), [typeof(double)])!;
    private static readonly MethodInfo DateTime_AddHours = typeof(DateTime).GetRuntimeMethod(nameof(DateTime.AddHours), [typeof(double)])!;
    private static readonly MethodInfo DateTime_AddMinutes = typeof(DateTime).GetRuntimeMethod(nameof(DateTime.AddMinutes), [typeof(double)])!;
    private static readonly MethodInfo DateTime_AddSeconds = typeof(DateTime).GetRuntimeMethod(nameof(DateTime.AddSeconds), [typeof(double)])!;
    private static readonly MethodInfo DateTime_AddMilliseconds = typeof(DateTime).GetRuntimeMethod(nameof(DateTime.AddMilliseconds), [typeof(double)])!;

    // DateTimeOffset Add
    private static readonly MethodInfo DateTimeOffset_AddYears = typeof(DateTimeOffset).GetRuntimeMethod(nameof(DateTimeOffset.AddYears), [typeof(int)])!;
    private static readonly MethodInfo DateTimeOffset_AddMonths = typeof(DateTimeOffset).GetRuntimeMethod(nameof(DateTimeOffset.AddMonths), [typeof(int)])!;
    private static readonly MethodInfo DateTimeOffset_AddDays = typeof(DateTimeOffset).GetRuntimeMethod(nameof(DateTimeOffset.AddDays), [typeof(double)])!;
    private static readonly MethodInfo DateTimeOffset_AddHours = typeof(DateTimeOffset).GetRuntimeMethod(nameof(DateTimeOffset.AddHours), [typeof(double)])!;
    private static readonly MethodInfo DateTimeOffset_AddMinutes = typeof(DateTimeOffset).GetRuntimeMethod(nameof(DateTimeOffset.AddMinutes), [typeof(double)])!;
    private static readonly MethodInfo DateTimeOffset_AddSeconds = typeof(DateTimeOffset).GetRuntimeMethod(nameof(DateTimeOffset.AddSeconds), [typeof(double)])!;
    private static readonly MethodInfo DateTimeOffset_AddMilliseconds = typeof(DateTimeOffset).GetRuntimeMethod(nameof(DateTimeOffset.AddMilliseconds), [typeof(double)])!;
    private static readonly MethodInfo DateTimeOffset_ToUnixTimeSeconds = typeof(DateTimeOffset).GetRuntimeMethod(nameof(DateTimeOffset.ToUnixTimeSeconds), Type.EmptyTypes)!;
    private static readonly MethodInfo DateTimeOffset_ToUnixTimeMilliseconds = typeof(DateTimeOffset).GetRuntimeMethod(nameof(DateTimeOffset.ToUnixTimeMilliseconds), Type.EmptyTypes)!;

    // DateOnly
    private static readonly MethodInfo DateOnly_FromDateTime = typeof(DateOnly).GetRuntimeMethod(nameof(DateOnly.FromDateTime), [typeof(DateTime)])!;
    private static readonly MethodInfo DateOnly_AddYears = typeof(DateOnly).GetRuntimeMethod(nameof(DateOnly.AddYears), [typeof(int)])!;
    private static readonly MethodInfo DateOnly_AddMonths = typeof(DateOnly).GetRuntimeMethod(nameof(DateOnly.AddMonths), [typeof(int)])!;
    private static readonly MethodInfo DateOnly_AddDays = typeof(DateOnly).GetRuntimeMethod(nameof(DateOnly.AddDays), [typeof(int)])!;

    // TimeOnly
    private static readonly MethodInfo TimeOnly_FromDateTime = typeof(TimeOnly).GetRuntimeMethod(nameof(TimeOnly.FromDateTime), [typeof(DateTime)])!;
    private static readonly MethodInfo TimeOnly_FromTimeSpan = typeof(TimeOnly).GetRuntimeMethod(nameof(TimeOnly.FromTimeSpan), [typeof(TimeSpan)])!;
    private static readonly MethodInfo TimeOnly_Add = typeof(TimeOnly).GetRuntimeMethod(nameof(TimeOnly.Add), [typeof(TimeSpan)])!;
    private static readonly MethodInfo TimeOnly_AddHours = typeof(TimeOnly).GetRuntimeMethod(nameof(TimeOnly.AddHours), [typeof(int)])!;
    private static readonly MethodInfo TimeOnly_AddMinutes = typeof(TimeOnly).GetRuntimeMethod(nameof(TimeOnly.AddMinutes), [typeof(int)])!;
    private static readonly MethodInfo TimeOnly_IsBetween = typeof(TimeOnly).GetRuntimeMethod(nameof(TimeOnly.IsBetween), [typeof(TimeOnly), typeof(TimeOnly)])!;
    private static readonly MethodInfo TimeOnly_ToTimeSpan = typeof(TimeOnly).GetRuntimeMethod(nameof(TimeOnly.ToTimeSpan), Type.EmptyTypes)!;

    public BigQueryDateTimeMethodTranslator(
        ISqlExpressionFactory sqlExpressionFactory,
        IRelationalTypeMappingSource typeMappingSource)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
        _typeMappingSource = typeMappingSource;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        // DateTime Add methods
        if (method == DateTime_AddYears)
            return DateTimeAdd(instance!, arguments[0], "YEAR", typeof(DateTime));
        if (method == DateTime_AddMonths)
            return DateTimeAdd(instance!, arguments[0], "MONTH", typeof(DateTime));
        if (method == DateTime_AddDays)
            return DateTimeAdd(instance!, arguments[0], "DAY", typeof(DateTime));
        if (method == DateTime_AddHours)
            return DateTimeAdd(instance!, arguments[0], "HOUR", typeof(DateTime));
        if (method == DateTime_AddMinutes)
            return DateTimeAdd(instance!, arguments[0], "MINUTE", typeof(DateTime));
        if (method == DateTime_AddSeconds)
            return DateTimeAdd(instance!, arguments[0], "SECOND", typeof(DateTime));
        if (method == DateTime_AddMilliseconds)
            return DateTimeAdd(instance!, arguments[0], "MILLISECOND", typeof(DateTime));

        // DateTimeOffset Add methods - use TIMESTAMP_ADD for BigQuery TIMESTAMP type
        if (method == DateTimeOffset_AddYears)
            return TimestampAdd(instance!, arguments[0], "YEAR", typeof(DateTimeOffset));
        if (method == DateTimeOffset_AddMonths)
            return TimestampAdd(instance!, arguments[0], "MONTH", typeof(DateTimeOffset));
        if (method == DateTimeOffset_AddDays)
            return TimestampAdd(instance!, arguments[0], "DAY", typeof(DateTimeOffset));
        if (method == DateTimeOffset_AddHours)
            return TimestampAdd(instance!, arguments[0], "HOUR", typeof(DateTimeOffset));
        if (method == DateTimeOffset_AddMinutes)
            return TimestampAdd(instance!, arguments[0], "MINUTE", typeof(DateTimeOffset));
        if (method == DateTimeOffset_AddSeconds)
            return TimestampAdd(instance!, arguments[0], "SECOND", typeof(DateTimeOffset));
        if (method == DateTimeOffset_AddMilliseconds)
            return TimestampAdd(instance!, arguments[0], "MILLISECOND", typeof(DateTimeOffset));

        // DateTimeOffset.ToUnixTimeSeconds/Milliseconds
        if (method == DateTimeOffset_ToUnixTimeSeconds)
            return _sqlExpressionFactory.Function(
                "UNIX_SECONDS",
                [instance!],
                nullable: true,
                argumentsPropagateNullability: TrueArrays1,
                typeof(long));

        if (method == DateTimeOffset_ToUnixTimeMilliseconds)
            return _sqlExpressionFactory.Function(
                "UNIX_MILLIS",
                [instance!],
                nullable: true,
                argumentsPropagateNullability: TrueArrays1,
                typeof(long));

        // DateOnly
        if (method == DateOnly_FromDateTime)
            return _sqlExpressionFactory.Function(
                "DATE",
                [arguments[0]],
                nullable: true,
                argumentsPropagateNullability: TrueArrays1,
                typeof(DateOnly));

        if (method == DateOnly_AddYears)
            return DateAdd(instance!, arguments[0], "YEAR", typeof(DateOnly));
        if (method == DateOnly_AddMonths)
            return DateAdd(instance!, arguments[0], "MONTH", typeof(DateOnly));
        if (method == DateOnly_AddDays)
            return DateAdd(instance!, arguments[0], "DAY", typeof(DateOnly));

        // TimeOnly
        if (method == TimeOnly_FromDateTime)
            return _sqlExpressionFactory.Function(
                "TIME",
                [arguments[0]],
                nullable: true,
                argumentsPropagateNullability: TrueArrays1,
                typeof(TimeOnly));

        if (method == TimeOnly_FromTimeSpan)
            return _sqlExpressionFactory.Function(
                "TIME",
                [arguments[0]],
                nullable: true,
                argumentsPropagateNullability: TrueArrays1,
                typeof(TimeOnly));

        if (method == TimeOnly_Add)
            return _sqlExpressionFactory.Function(
                "TIME_ADD",
                [instance!, _sqlExpressionFactory.Fragment($"INTERVAL"), arguments[0]],
                nullable: true,
                argumentsPropagateNullability: TrueArrays2,
                typeof(TimeOnly));

        if (method == TimeOnly_AddHours)
            return TimeAdd(instance!, arguments[0], "HOUR", typeof(TimeOnly));
        if (method == TimeOnly_AddMinutes)
            return TimeAdd(instance!, arguments[0], "MINUTE", typeof(TimeOnly));

        if (method == TimeOnly_IsBetween)
            return _sqlExpressionFactory.AndAlso(
                _sqlExpressionFactory.GreaterThanOrEqual(instance!, arguments[0]),
                _sqlExpressionFactory.LessThan(instance!, arguments[1]));

        if (method == TimeOnly_ToTimeSpan)
            return instance;

        return null;
    }

    private SqlExpression DateTimeAdd(SqlExpression instance, SqlExpression value, string part, Type returnType)
    {
        // DATETIME_ADD(datetime_expression, INTERVAL int64 date_part)
        var intValue = value.Type == typeof(double) || value.Type == typeof(int)
            ? _sqlExpressionFactory.Convert(value, typeof(long))
            : value;

        return _sqlExpressionFactory.Function(
            "DATETIME_ADD",
            [instance, _sqlExpressionFactory.Fragment($"INTERVAL"), intValue, _sqlExpressionFactory.Fragment(part)],
            nullable: true,
            argumentsPropagateNullability: TrueArrays2,
            returnType);
    }

    private SqlExpression TimestampAdd(SqlExpression instance, SqlExpression value, string part, Type returnType)
    {
        // TIMESTAMP_ADD(timestamp_expression, INTERVAL int64 date_part)
        var intValue = value.Type == typeof(double) || value.Type == typeof(int)
            ? _sqlExpressionFactory.Convert(value, typeof(long))
            : value;

        return _sqlExpressionFactory.Function(
            "TIMESTAMP_ADD",
            [instance, _sqlExpressionFactory.Fragment($"INTERVAL"), intValue, _sqlExpressionFactory.Fragment(part)],
            nullable: true,
            argumentsPropagateNullability: TrueArrays2,
            returnType);
    }

    private SqlExpression DateAdd(SqlExpression instance, SqlExpression value, string part, Type returnType)
    {
        // DATE_ADD(date_expression, INTERVAL int64 date_part)
        var intValue = value.Type == typeof(int)
            ? _sqlExpressionFactory.Convert(value, typeof(long))
            : value;

        return _sqlExpressionFactory.Function(
            "DATE_ADD",
            [instance, _sqlExpressionFactory.Fragment($"INTERVAL"), intValue, _sqlExpressionFactory.Fragment(part)],
            nullable: true,
            argumentsPropagateNullability: TrueArrays2,
            returnType);
    }

    private SqlExpression TimeAdd(SqlExpression instance, SqlExpression value, string part, Type returnType)
    {
        // TIME_ADD(time_expression, INTERVAL int64 time_part)
        var intValue = value.Type == typeof(int)
            ? _sqlExpressionFactory.Convert(value, typeof(long))
            : value;

        return _sqlExpressionFactory.Function(
            "TIME_ADD",
            [instance, _sqlExpressionFactory.Fragment($"INTERVAL"), intValue, _sqlExpressionFactory.Fragment(part)],
            nullable: true,
            argumentsPropagateNullability: TrueArrays2,
            returnType);
    }
}