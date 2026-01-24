using Ivy.Data.BigQuery;
using Xunit;
using Xunit.Abstractions;

namespace Ivy.EntityFrameworkCore.BigQuery.Tests.Storage;

/// <summary>
/// Tests to verify the correct TIMESTAMP literal format for BigQuery.
/// These tests require a valid BigQuery connection (set BQ_EFCORE_TEST_CONN_STRING environment variable).
/// </summary>
public class BigQueryTimestampFormatTest
{
    private readonly ITestOutputHelper _output;

    public BigQueryTimestampFormatTest(ITestOutputHelper output)
    {
        _output = output;
    }

    private string? GetConnectionString()
    {
        return Environment.GetEnvironmentVariable("BQ_EFCORE_TEST_CONN_STRING");
    }

    [Fact]
    public async Task Test_Timestamp_Format_Without_Space()
    {
        var connString = GetConnectionString();
        if (string.IsNullOrEmpty(connString))
        {
            _output.WriteLine("Skipping test - BQ_EFCORE_TEST_CONN_STRING not set");
            return;
        }

        // Format WITHOUT space: 2008-12-25 15:30:00+00:00
        var sql = "SELECT TIMESTAMP '2008-12-25 15:30:00+00:00' AS ts";
        _output.WriteLine($"Testing SQL: {sql}");

        await using var conn = new BigQueryConnection(connString);
        await conn.OpenAsync();

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;

        try
        {
            await using var reader = await cmd.ExecuteReaderAsync();
            if (await reader.ReadAsync())
            {
                var result = reader.GetValue(0);
                _output.WriteLine($"SUCCESS - Format without space works. Result: {result}");
            }
        }
        catch (Exception ex)
        {
            _output.WriteLine($"FAILED - Format without space: {ex.Message}");
            throw;
        }
    }

    [Fact]
    public async Task Test_Timestamp_Format_With_Space()
    {
        var connString = GetConnectionString();
        if (string.IsNullOrEmpty(connString))
        {
            _output.WriteLine("Skipping test - BQ_EFCORE_TEST_CONN_STRING not set");
            return;
        }

        // Format WITH space: 2008-12-25 15:30:00 +00:00
        var sql = "SELECT TIMESTAMP '2008-12-25 15:30:00 +00:00' AS ts";
        _output.WriteLine($"Testing SQL: {sql}");

        await using var conn = new BigQueryConnection(connString);
        await conn.OpenAsync();

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;

        try
        {
            await using var reader = await cmd.ExecuteReaderAsync();
            if (await reader.ReadAsync())
            {
                var result = reader.GetValue(0);
                _output.WriteLine($"SUCCESS - Format with space works. Result: {result}");
            }
        }
        catch (Exception ex)
        {
            _output.WriteLine($"FAILED - Format with space: {ex.Message}");
            throw;
        }
    }

    [Fact]
    public async Task Test_Timestamp_Format_Google_Docs_Example()
    {
        var connString = GetConnectionString();
        if (string.IsNullOrEmpty(connString))
        {
            _output.WriteLine("Skipping test - BQ_EFCORE_TEST_CONN_STRING not set");
            return;
        }

        // Google docs example format: 2008-12-25 15:30:00+00 (no colon in offset)
        var sql = "SELECT TIMESTAMP '2008-12-25 15:30:00+00' AS ts";
        _output.WriteLine($"Testing SQL: {sql}");

        await using var conn = new BigQueryConnection(connString);
        await conn.OpenAsync();

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;

        try
        {
            await using var reader = await cmd.ExecuteReaderAsync();
            if (await reader.ReadAsync())
            {
                var result = reader.GetValue(0);
                _output.WriteLine($"SUCCESS - Google docs format works. Result: {result}");
            }
        }
        catch (Exception ex)
        {
            _output.WriteLine($"FAILED - Google docs format: {ex.Message}");
            throw;
        }
    }

    [Fact]
    public async Task Test_Timestamp_Format_With_Microseconds_No_Space()
    {
        var connString = GetConnectionString();
        if (string.IsNullOrEmpty(connString))
        {
            _output.WriteLine("Skipping test - BQ_EFCORE_TEST_CONN_STRING not set");
            return;
        }

        // Format that matches EF Core type mapping output (no space): 2008-12-25 15:30:00.123456+05:30
        var sql = "SELECT TIMESTAMP '2008-12-25 15:30:00.123456+05:30' AS ts";
        _output.WriteLine($"Testing SQL: {sql}");

        await using var conn = new BigQueryConnection(connString);
        await conn.OpenAsync();

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;

        try
        {
            await using var reader = await cmd.ExecuteReaderAsync();
            if (await reader.ReadAsync())
            {
                var result = reader.GetValue(0);
                _output.WriteLine($"SUCCESS - Microseconds format without space works. Result: {result}");
            }
        }
        catch (Exception ex)
        {
            _output.WriteLine($"FAILED - Microseconds format without space: {ex.Message}");
            throw;
        }
    }

    [Fact]
    public async Task Test_Timestamp_Format_With_Microseconds_With_Space()
    {
        var connString = GetConnectionString();
        if (string.IsNullOrEmpty(connString))
        {
            _output.WriteLine("Skipping test - BQ_EFCORE_TEST_CONN_STRING not set");
            return;
        }

        // Format with space before timezone: 2008-12-25 15:30:00.123456 +05:30
        var sql = "SELECT TIMESTAMP '2008-12-25 15:30:00.123456 +05:30' AS ts";
        _output.WriteLine($"Testing SQL: {sql}");

        await using var conn = new BigQueryConnection(connString);
        await conn.OpenAsync();

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;

        try
        {
            await using var reader = await cmd.ExecuteReaderAsync();
            if (await reader.ReadAsync())
            {
                var result = reader.GetValue(0);
                _output.WriteLine($"SUCCESS - Microseconds format with space works. Result: {result}");
            }
        }
        catch (Exception ex)
        {
            _output.WriteLine($"FAILED - Microseconds format with space: {ex.Message}");
            throw;
        }
    }
}
