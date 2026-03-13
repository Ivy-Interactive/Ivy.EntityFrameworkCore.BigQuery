using AdoNet.Specification.Tests;

namespace Ivy.Data.BigQuery.Conformance.Tests;

public class Util
{
    public static void ExecuteNonQuery(IDbFactoryFixture factoryFixture, string sql)
    {
        using var connection = factoryFixture.Factory.CreateConnection()
            ?? throw new InvalidOperationException("Failed to create connection");
        connection.ConnectionString = Environment.GetEnvironmentVariable("ConnectionString") ?? factoryFixture.ConnectionString;
        connection.Open();

        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }
}