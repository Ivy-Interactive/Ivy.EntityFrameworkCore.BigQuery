using Microsoft.Extensions.Configuration;

namespace Microsoft.EntityFrameworkCore.TestUtilities;

public static class TestEnvironment
{
    public static IConfiguration Config { get; } = new ConfigurationBuilder()
        .SetBasePath(Directory.GetCurrentDirectory())
        .AddJsonFile("config.json", optional: true)
        .AddJsonFile("config.test.json", optional: true)
        .AddEnvironmentVariables()
        .Build()
        .GetSection("Test:SqlServer");

    private const string DefaultConnectionString = "DataSource=http://localhost:9050;AuthMethod=ApplicationDefaultCredentials;ProjectId=test;";

    public static string DefaultConnection { get; } =
            Environment.GetEnvironmentVariable("BQ_EFCORE_TEST_CONN_STRING", EnvironmentVariableTarget.User)
            ?? Config["DefaultConnection"]
            ?? DefaultConnectionString;
}
