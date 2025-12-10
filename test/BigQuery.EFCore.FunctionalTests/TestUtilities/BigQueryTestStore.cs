using Ivy.Data.BigQuery;
using Ivy.EntityFrameworkCore.BigQuery.Extensions;
using Microsoft.CodeAnalysis;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.TestUtilities;
using System.Data;
using System.Data.Common;
using System.Text.RegularExpressions;

namespace Ivy.EntityFrameworkCore.BigQuery.TestUtilities;

public class BigQueryTestStore : RelationalTestStore
{
    private static readonly SemaphoreSlim Semaphore = new(1, 1);

    private readonly string _testDatasetName;
    private string? _scriptPath;
    public const int CommandTimeout = 300;

    public BigQueryTestStore(string name, bool shared = true, string? scriptPath = null) : base(name, shared, CreateConnection(name))
    {
        _testDatasetName = name;
        if (scriptPath != null)
        {
            _scriptPath = Path.Combine(Path.GetDirectoryName(typeof(BigQueryTestStore).Assembly.Location)!, scriptPath);
        }
    }

    public static BigQueryTestStore GetOrCreate(
        string name,
        string? scriptPath = null,
        string? additionalSql = null,
        string? connectionStringOptions = null,
        bool useConnectionString = false)
        => new(name, shared: true, scriptPath);

    private static BigQueryConnection CreateConnection(string name)
        => new(CreateConnectionString(name));

    public new BigQueryConnection Connection => (BigQueryConnection)base.Connection;

    public static string CreateConnectionString(string name, string? options = null)
    {
        var builder = new BigQueryConnectionStringBuilder(TestEnvironment.DefaultConnection)
        {
            DefaultDatasetId = name
        };
        return builder.ConnectionString;
    }

    public override DbContextOptionsBuilder AddProviderOptions(DbContextOptionsBuilder builder)
        => builder.UseBigQuery(Connection.ConnectionString);

    protected override async Task InitializeAsync(Func<DbContext> createContext, Func<DbContext, Task>? seed, Func<DbContext, Task>? clean)
    {
        if (await CreateDatabaseAsync(createContext, clean))
        {
            if (_scriptPath != null)
            {
                ExecuteScript(_scriptPath);
            }
            else
            {
                await using var seedContext = createContext();
                seedContext.Database.SetConnectionString(CreateConnectionString(_testDatasetName));
                if (seed != null)
                {
                    await seed(seedContext);
                }
            }
        }

        //Connection.ConnectionString = CreateConnectionString(_testDatasetName);
    }

    private async Task<bool> CreateDatabaseAsync(Func<DbContext> createContext, Func<DbContext, Task>? clean)
    {
        await using var seedContext = createContext();
        seedContext.Database.SetConnectionString(CreateConnectionString(_testDatasetName));
        var databaseCreator = (RelationalDatabaseCreator)seedContext.Database.GetService<IDatabaseCreator>();

        var connection = seedContext.Database.GetDbConnection();

        if (await databaseCreator.ExistsAsync())
        {
            if (_scriptPath != null)
            {
                return false;
            }

            if (clean != null)
            {
                await clean(seedContext);
            }
            await CleanAsync(seedContext);
            return true;
        }

        await databaseCreator.CreateAsync();

        if (_scriptPath == null)
        {
            await seedContext.Database.EnsureCreatedAsync();
        }

        return true;
    }

    public void ExecuteScript(string scriptPath)
    {
        var script = File.ReadAllText(scriptPath);
        Execute(
            Connection, command =>
            {
                foreach (var batch in
                         new Regex("^GO", RegexOptions.IgnoreCase | RegexOptions.Multiline, TimeSpan.FromMilliseconds(1000.0))
                             .Split(script).Where(b => !string.IsNullOrEmpty(b)))
                {
                    command.CommandText = batch;
                    command.ExecuteNonQuery();
                }

                return 0;
            }, "");
    }

    private static T Execute<T>(
    DbConnection connection,
    Func<DbCommand, T> execute,
    string sql,
    bool useTransaction = false,
    object[]? parameters = null)
     => ExecuteCommand(connection, execute, sql, parameters);

    private static Task<T> ExecuteAsync<T>(
        DbConnection connection,
        Func<DbCommand, Task<T>> executeAsync,
        string sql,
        IReadOnlyList<object>? parameters = null)
        => ExecuteCommandAsync(connection, executeAsync, sql, parameters);

    private static Task<T> ExecuteAsync<T>(
        DbConnection connection,
        Func<DbCommand, Task<T>> executeAsync,
        string sql,
        bool useTransaction = false,
        IReadOnlyList<object>? parameters = null)
        => ExecuteCommandAsync(connection, executeAsync, sql, useTransaction, parameters);

    private static T ExecuteCommand<T>(
        DbConnection connection,
        Func<DbCommand, T> execute,
        string sql,
        object[]? parameters)
    {
        if (connection.State != ConnectionState.Closed)
        {
            connection.Close();
        }

        connection.Open();
        try
        {

            T result;
            using (var command = CreateCommand(connection, sql, parameters))
            {
                result = execute(command);
            }

            return result;
        }
        finally
        {
            if (connection.State == ConnectionState.Closed
                && connection.State != ConnectionState.Closed)
            {
                connection.Close();
            }
        }
    }

    private static async Task<T> ExecuteCommandAsync<T>(
        DbConnection connection,
        Func<DbCommand, Task<T>> executeAsync,
        string sql,
    IReadOnlyList<object>? parameters)
    {
        if (connection.State != ConnectionState.Closed)
        {
            await connection.CloseAsync();
        }

        await connection.OpenAsync();
        try
        {

            T result;
            using (var command = CreateCommand(connection, sql, parameters))
            {
                result = await executeAsync(command);
            }

            return result;
        }
        finally
        {
            if (connection.State != ConnectionState.Closed)
            {
                await connection.CloseAsync();
            }
        }
    }

    private static async Task<T> ExecuteCommandAsync<T>(
        DbConnection connection,
        Func<DbCommand, Task<T>> executeAsync,
        string sql,
        bool useTransaction,
        IReadOnlyList<object>? parameters)
    {
        if (connection.State != ConnectionState.Closed)
        {
            await connection.CloseAsync();
        }

        await connection.OpenAsync();
        try
        {
            await using var transaction = useTransaction ? await connection.BeginTransactionAsync() : null;

            T result;
            await using (var command = CreateCommand(connection, sql, parameters))
            {
                result = await executeAsync(command);
            }

            if (transaction is not null)
            {
                await transaction.CommitAsync();
            }

            return result;
        }
        finally
        {
            if (connection.State == ConnectionState.Closed
                && connection.State != ConnectionState.Closed)
            {
                await connection.CloseAsync();
            }
        }
    }

    public Task<int> ExecuteNonQueryAsync(string sql, params object[] parameters)
        => ExecuteNonQueryAsync(Connection, sql, parameters);

    private static Task<int> ExecuteNonQueryAsync(DbConnection connection, string sql, object[]? parameters = null)
    => ExecuteAsync(connection, command => command.ExecuteNonQueryAsync(), sql, false, parameters);

    private static DbCommand CreateCommand(
        DbConnection connection,
        string commandText,
        IReadOnlyList<object>? parameters = null)
    {
        var command = (BigQueryCommand)connection.CreateCommand();

        command.CommandText = commandText;
        command.CommandTimeout = CommandTimeout;

        if (parameters != null)
        {
            for (var i = 0; i < parameters.Count; i++)
            {
                command.Parameters.AddWithValue("@" + i, parameters[i]);
            }
        }

        return command;
    }

    private static Task<T> ExecuteScalarAsync<T>(DbConnection connection, string sql, IReadOnlyList<object>? parameters = null)
    => ExecuteAsync(connection, async command => (T)(await command.ExecuteScalarAsync())!, sql, parameters);

    public override Task CleanAsync(DbContext context)
    {
        context.Database.EnsureClean();
        return Task.CompletedTask;
    }

    public int ExecuteNonQuery(string sql, params object[] parameters)
    {
        using var command = CreateCommand(sql, parameters);
        return command.ExecuteNonQuery();
    }
    private DbCommand CreateCommand(string commandText, object[] parameters)
    {
        var command = (BigQueryCommand)Connection.CreateCommand();

        command.CommandText = commandText;
        command.CommandTimeout = CommandTimeout;

        for (var i = 0; i < parameters.Length; i++)
        {
            command.Parameters.AddWithValue("@p" + i, parameters[i]);
        }

        return command;
    }

    public override string NormalizeDelimitersInRawString(string sql)
       => sql.Replace("[", OpenDelimiter).Replace("]", CloseDelimiter);


    protected override string OpenDelimiter
        => "`";

    protected override string CloseDelimiter
        => "`";

    public override void Dispose()
    {
        using var controlConnection = new BigQueryConnection(TestEnvironment.DefaultConnection);
        controlConnection.Open();
        using var dropCmd = controlConnection.CreateCommand();
        dropCmd.CommandText = $"DROP SCHEMA IF EXISTS `{_testDatasetName}` CASCADE";
        dropCmd.ExecuteNonQuery();

        base.Dispose();
    }
}