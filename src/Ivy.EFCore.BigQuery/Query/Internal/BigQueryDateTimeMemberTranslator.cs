using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal;

/// <summary>
/// Translates DateTime, DateTimeOffset, DateOnly, TimeOnly, and TimeSpan member access to BigQuery SQL.
/// </summary>
public class BigQueryDateTimeMemberTranslator : IMemberTranslator
{
    private readonly ISqlExpressionFactory _sqlExpressionFactory;
    private readonly IRelationalTypeMappingSource _typeMappingSource;

    private static readonly bool[] TrueArrays1 = [true];
    private static readonly bool[] TrueArrays2 = [true, true];

    public BigQueryDateTimeMemberTranslator(
        ISqlExpressionFactory sqlExpressionFactory,
        IRelationalTypeMappingSource typeMappingSource)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
        _typeMappingSource = typeMappingSource;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MemberInfo member,
        Type returnType,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        var declaringType = member.DeclaringType;

        if (declaringType != typeof(DateTime)
            && declaringType != typeof(DateTimeOffset)
            && declaringType != typeof(DateOnly)
            && declaringType != typeof(TimeOnly)
            && declaringType != typeof(TimeSpan))
        {
            return null;
        }

        if (instance is null)
        {
            return member.Name switch
            {
                nameof(DateTime.Now) or nameof(DateTimeOffset.Now)
                    => _sqlExpressionFactory.Function(
                        "CURRENT_DATETIME",
                        [],
                        nullable: false,
                        argumentsPropagateNullability: [],
                        returnType),

                nameof(DateTime.UtcNow) or nameof(DateTimeOffset.UtcNow)
                    => _sqlExpressionFactory.Function(
                        "CURRENT_TIMESTAMP",
                        [],
                        nullable: false,
                        argumentsPropagateNullability: [],
                        returnType),

                nameof(DateTime.Today)
                    => _sqlExpressionFactory.Function(
                        "CURRENT_DATE",
                        [],
                        nullable: false,
                        argumentsPropagateNullability: [],
                        typeof(DateTime)),

                _ => null
            };
        }

        return member.Name switch
        {
            // Date/Time component extraction - works for DateTime, DateTimeOffset, DateOnly, TimeOnly
            nameof(DateTime.Year) => Extract(instance, "YEAR", returnType),
            nameof(DateTime.Month) => Extract(instance, "MONTH", returnType),
            nameof(DateTime.Day) => Extract(instance, "DAY", returnType),
            nameof(DateTime.Hour) => Extract(instance, "HOUR", returnType),
            nameof(DateTime.Minute) => Extract(instance, "MINUTE", returnType),
            nameof(DateTime.Second) => Extract(instance, "SECOND", returnType),
            nameof(DateTime.Millisecond) => ExtractMillisecond(instance, returnType),
            nameof(DateTime.DayOfYear) => Extract(instance, "DAYOFYEAR", returnType),
            nameof(DateTime.DayOfWeek) => ExtractDayOfWeek(instance, returnType),

            // DateTime.Date - extract date part
            nameof(DateTime.Date) when declaringType == typeof(DateTime)
                => _sqlExpressionFactory.Function(
                    "DATE",
                    [instance],
                    nullable: true,
                    argumentsPropagateNullability: TrueArrays1,
                    typeof(DateTime)),

            // DateTimeOffset.Date - extract date part
            nameof(DateTimeOffset.Date) when declaringType == typeof(DateTimeOffset)
                => _sqlExpressionFactory.Function(
                    "DATE",
                    [instance],
                    nullable: true,
                    argumentsPropagateNullability: TrueArrays1,
                    typeof(DateTime)),

            // DateTime.TimeOfDay / DateTimeOffset.TimeOfDay - extract time part
            nameof(DateTime.TimeOfDay)
                => _sqlExpressionFactory.Function(
                    "TIME",
                    [instance],
                    nullable: true,
                    argumentsPropagateNullability: TrueArrays1,
                    typeof(TimeSpan)),

            // DateTimeOffset specific
            nameof(DateTimeOffset.DateTime) when declaringType == typeof(DateTimeOffset)
                => _sqlExpressionFactory.Function(
                    "DATETIME",
                    [instance],
                    nullable: true,
                    argumentsPropagateNullability: TrueArrays1,
                    typeof(DateTime)),

            nameof(DateTimeOffset.UtcDateTime) when declaringType == typeof(DateTimeOffset)
                => _sqlExpressionFactory.Function(
                    "DATETIME",
                    [instance],
                    nullable: true,
                    argumentsPropagateNullability: TrueArrays1,
                    typeof(DateTime)),

            // TimeSpan properties
            nameof(TimeSpan.Hours) when declaringType == typeof(TimeSpan)
                => Extract(instance, "HOUR", returnType),
            nameof(TimeSpan.Minutes) when declaringType == typeof(TimeSpan)
                => Extract(instance, "MINUTE", returnType),
            nameof(TimeSpan.Seconds) when declaringType == typeof(TimeSpan)
                => Extract(instance, "SECOND", returnType),
            nameof(TimeSpan.Milliseconds) when declaringType == typeof(TimeSpan)
                => ExtractMillisecond(instance, returnType),

            // 1 tick = 100 nanoseconds, 1 microsecond = 10 ticks
            nameof(TimeSpan.Ticks) when declaringType == typeof(TimeSpan)
                => _sqlExpressionFactory.Multiply(
                    _sqlExpressionFactory.Function(
                        "UNIX_MICROS",
                        [instance],
                        nullable: true,
                        argumentsPropagateNullability: TrueArrays1,
                        typeof(long)),
                    _sqlExpressionFactory.Constant(10L)),

            _ => null
        };
    }

    private SqlExpression Extract(SqlExpression instance, string part, Type returnType)
    {
        // EXTRACT(part FROM date_expression)
        var int64Mapping = _typeMappingSource.FindMapping(typeof(long));
        return _sqlExpressionFactory.Convert(
            new BigQueryExtractExpression(part, instance, typeof(long), int64Mapping),
            returnType);
    }

    private SqlExpression ExtractMillisecond(SqlExpression instance, Type returnType)
    {
        // EXTRACT(MILLISECOND FROM x)
        var int64Mapping = _typeMappingSource.FindMapping(typeof(long));
        var milliseconds = new BigQueryExtractExpression("MILLISECOND", instance, typeof(long), int64Mapping);

        return _sqlExpressionFactory.Convert(
            _sqlExpressionFactory.Modulo(milliseconds, _sqlExpressionFactory.Constant(1000L)),
            returnType);
    }

    private SqlExpression ExtractDayOfWeek(SqlExpression instance, Type returnType)
    {
        // BigQuery EXTRACT(DAYOFWEEK FROM x) returns 1 (Sunday) to 7 (Saturday)
        var int64Mapping = _typeMappingSource.FindMapping(typeof(long));
        var dayOfWeek = new BigQueryExtractExpression("DAYOFWEEK", instance, typeof(long), int64Mapping);

        return _sqlExpressionFactory.Convert(
            _sqlExpressionFactory.Subtract(dayOfWeek, _sqlExpressionFactory.Constant(1L)),
            returnType);
    }
}