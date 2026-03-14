using System.Data.Common;

namespace Ivy.Data.BigQuery;

public sealed class BigQueryProviderFactory : DbProviderFactory
{
    public static readonly BigQueryProviderFactory Instance = new();

    private BigQueryProviderFactory()
    {
    }

    public override DbCommand CreateCommand()
    {
        return new BigQueryCommand();
    }

    public override DbConnection CreateConnection()
    {
        return new BigQueryConnection();
    }

    public override DbParameter CreateParameter()
    {
        return new BigQueryParameter();
    }

    public override DbConnectionStringBuilder CreateConnectionStringBuilder()
    {
        return new BigQueryConnectionStringBuilder();
    }
    
    public override DbCommandBuilder? CreateCommandBuilder()
    {
        // Todo?
        return null;
    }

    public override DbDataAdapter? CreateDataAdapter()
    {
        // Todo?
        return null;
    }

    public override bool CanCreateDataAdapter => false; 

    public override bool CanCreateCommandBuilder => false; 

    public override bool CanCreateDataSourceEnumerator => false;
}