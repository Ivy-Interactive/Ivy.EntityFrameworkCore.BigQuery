using Ivy.EntityFrameworkCore.BigQuery.Diagnostics;
using Ivy.EntityFrameworkCore.BigQuery.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.Extensions.DependencyInjection;

namespace Ivy.EntityFrameworkCore.BigQuery.TestUtilities;

public class BigQueryTestHelpers : RelationalTestHelpers
{
    protected BigQueryTestHelpers() { }

    public static BigQueryTestHelpers Instance { get; } = new();

    public override IServiceCollection AddProviderServices(IServiceCollection services)
        => services.AddEntityFrameworkBigQuery();

    public override DbContextOptionsBuilder UseProviderOptions(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.UseBigQuery("DefaultDatasetId=DummyDataset");

    public override LoggingDefinitions LoggingDefinitions { get; } = new BigQueryLoggingDefinitions();
}