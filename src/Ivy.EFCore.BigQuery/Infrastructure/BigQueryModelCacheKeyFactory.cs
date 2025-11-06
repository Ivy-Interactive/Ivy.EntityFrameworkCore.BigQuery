using Ivy.EntityFrameworkCore.BigQuery.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace Ivy.EntityFrameworkCore.BigQuery.Infrastructure;

public class BigQueryModelCacheKeyFactory : IModelCacheKeyFactory
{
    public object Create(DbContext context, bool designTime)
        => new BigQueryModelCacheKey(context, designTime);

    public object Create(DbContext context) => new BigQueryModelCacheKey(context, false);
}


public class BigQueryModelCacheKey : ModelCacheKey
{
    private readonly string _defaultSchema;

    public BigQueryModelCacheKey(DbContext context, bool designTime)
        : base(context, designTime)
    {
        var options = context.GetService<DbContextOptions>();
        var bigQueryOptions = options.FindExtension<BigQueryOptionsExtension>();
        if (bigQueryOptions != null)
        {
            _defaultSchema = bigQueryOptions.DefaultDataset;
        }
    }

    protected override bool Equals(ModelCacheKey other)
        => base.Equals(other)
           && other is BigQueryModelCacheKey otherKey
           && _defaultSchema == otherKey._defaultSchema;

    public override int GetHashCode()
    {
        var hashCode = new HashCode();
        hashCode.Add(base.GetHashCode());
        hashCode.Add(_defaultSchema);
        return hashCode.ToHashCode();
    }
}