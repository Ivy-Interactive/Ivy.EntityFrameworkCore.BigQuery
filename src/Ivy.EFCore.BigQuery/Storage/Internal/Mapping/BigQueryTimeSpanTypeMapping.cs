using System.Data.Common;
using System.Globalization;
using Google.Cloud.BigQuery.V2;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace Ivy.EntityFrameworkCore.BigQuery.Storage.Internal.Mapping;

/// <summary>
/// Maps <see cref="TimeSpan"/> to BigQuery INT64 (stored as microseconds).
/// </summary>
/// <remarks>
/// BigQuery doesn't have a native interval/duration type like PostgreSQL.
/// We store TimeSpan as INT64 microseconds, which allows for precise arithmetic
/// and supports the full range of TimeSpan values.
/// </remarks>
public class BigQueryTimeSpanTypeMapping : RelationalTypeMapping
{
    private const long TicksPerMicrosecond = TimeSpan.TicksPerMillisecond / 1000; // 10

    private static readonly ValueConverter<TimeSpan, long> _converter =
        new(
            timeSpan => timeSpan.Ticks / TicksPerMicrosecond,
            microseconds => TimeSpan.FromTicks(microseconds * TicksPerMicrosecond));

    public BigQueryTimeSpanTypeMapping(string storeType = "INT64")
        : base(
            new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(
                    typeof(TimeSpan),
                    _converter),
                storeType,
                StoreTypePostfix.None,
                System.Data.DbType.Int64))
    {
    }

    protected BigQueryTimeSpanTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new BigQueryTimeSpanTypeMapping(parameters);

    /// <summary>
    /// Override to bypass base class sanitization which uses Convert.ChangeType
    /// (TimeSpan doesn't implement IConvertible, so Convert.ChangeType fails).
    /// </summary>
    public override string GenerateSqlLiteral(object? value)
    {
        if (value == null)
        {
            return "NULL";
        }

        return GenerateNonNullSqlLiteral(value);
    }

    protected override string GenerateNonNullSqlLiteral(object value)
    {
        // Value may be TimeSpan (model value) or long (provider value after conversion)
        var microseconds = value switch
        {
            TimeSpan ts => ts.Ticks / TicksPerMicrosecond,
            long l => l,
            _ => Convert.ToInt64(value)
        };
        return microseconds.ToString(CultureInfo.InvariantCulture);
    }

    protected override void ConfigureParameter(DbParameter parameter)
    {
        base.ConfigureParameter(parameter);

        if (parameter is Ivy.Data.BigQuery.BigQueryParameter bqParam)
        {
            bqParam.BigQueryDbType = BigQueryDbType.Int64;

            if (parameter.Value is TimeSpan ts)
            {
                // Convert TimeSpan to microseconds
                parameter.Value = ts.Ticks / TicksPerMicrosecond;
            }
        }
    }

    /// <summary>
    /// Converts microseconds (INT64) back to TimeSpan when reading from database.
    /// </summary>
    public static TimeSpan FromMicroseconds(long microseconds)
        => TimeSpan.FromTicks(microseconds * TicksPerMicrosecond);

    /// <summary>
    /// Converts TimeSpan to microseconds for storage.
    /// </summary>
    public static long ToMicroseconds(TimeSpan timeSpan)
        => timeSpan.Ticks / TicksPerMicrosecond;
}
