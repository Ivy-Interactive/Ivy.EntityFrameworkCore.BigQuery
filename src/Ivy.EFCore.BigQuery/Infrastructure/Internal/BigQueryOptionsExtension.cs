using Ivy.Data.BigQuery;
using Ivy.EntityFrameworkCore.BigQuery.Extensions;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;

namespace Ivy.EntityFrameworkCore.BigQuery.Infrastructure.Internal
{
    public class BigQueryOptionsExtension : RelationalOptionsExtension
    {
        //public string? ProjectId { get; private set; }
        //public string? CredentialsPath { get; private set; }

        private DbContextOptionsExtensionInfo? _info;
        public string? DefaultDataset;

        public BigQueryOptionsExtension()
        {

        }

        //public BigQueryOptionsExtension(string projectId, string datasetId)
        //{
        //    ProjectId = projectId;
        //    DatasetId = datasetId;
        //}

        protected BigQueryOptionsExtension(BigQueryOptionsExtension copyFrom)
            : base(copyFrom)
        {
            DefaultDataset = copyFrom.DefaultDataset;
        }

        public override bool Equals(object? obj)
        {
            return obj is BigQueryOptionsExtension other
                && base.Equals(other);
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }

        public override RelationalOptionsExtension WithConnectionString(string? connectionString)
        {
            var clone = (BigQueryOptionsExtension)base.WithConnectionString(connectionString);

            return clone;
        }

        public override void Validate(IDbContextOptions options)
        {
            base.Validate(options);

            //if (string.IsNullOrWhiteSpace(ConnectionString))
            //{
            //    throw new InvalidOperationException(
            //        $"No connection string was specified. Configure the BigQuery connection string by calling UseBigQuery() on the options builder.");
            //}
            //try
            //{
            //    var builder = new BigQueryConnectionStringBuilder(ConnectionString);
            //    if (string.IsNullOrWhiteSpace(builder.ProjectId))
            //    {
            //        throw new InvalidOperationException("The connection string must include a 'ProjectId'.");
            //    }
            //    if (builder.AuthMethod == BigQueryConnectionStringBuilder.BigQueryAuthMethod.JsonCredentials && string.IsNullOrWhiteSpace(builder.CredentialsFile))
            //    {
            //        throw new InvalidOperationException("The connection string must include a 'CredentialsFile' when using 'JsonCredentials' authentication method.");
            //    }
            //}
            //catch (ArgumentException ex)
            //{
            //    throw new InvalidOperationException($"The connection string is invalid: {ex.Message}", ex);
            //}
        }

        protected override RelationalOptionsExtension Clone()
        => new BigQueryOptionsExtension(this);

        public override void ApplyServices(IServiceCollection services)
        {
            services.AddEntityFrameworkBigQuery();
        }

        public override DbContextOptionsExtensionInfo Info => _info ??= new BigQueryOptionsExtensionInfo(this);

        private class BigQueryOptionsExtensionInfo : RelationalExtensionInfo
        {
            public BigQueryOptionsExtensionInfo(IDbContextOptionsExtension extension) : base(extension) { }

            public override bool IsDatabaseProvider => true;

            public override string LogFragment => "BigQuery Provider";

            public override int GetServiceProviderHashCode()
            {
                var extension = (BigQueryOptionsExtension)Extension;
                var hashCode = new HashCode();
                hashCode.Add(base.GetServiceProviderHashCode());
                hashCode.Add(extension.ConnectionString);
                return hashCode.ToHashCode();
            }

            public override void PopulateDebugInfo(IDictionary<string, string> debugInfo)
            {
                var csb = new BigQueryConnectionStringBuilder(Extension.ConnectionString);
                debugInfo["BigQueryExtension:ProjectId"] = csb.ProjectId ?? "(none)";
                debugInfo["BigQueryExtension:DefaultDataset"] = csb.DefaultDatasetId ?? "(none)";
                debugInfo["BigQueryExtension:AuthMethod"] = csb.AuthMethod.ToString();
                if (!string.IsNullOrWhiteSpace(csb.DefaultDatasetId))
                {
                    debugInfo["BigQueryExtension:DefaultDatasetId"] = csb.DefaultDatasetId;
                }
            }

            public override bool ShouldUseSameServiceProvider(DbContextOptionsExtensionInfo other)
            {
                return other is BigQueryOptionsExtensionInfo otherBigQuery
                    && base.ShouldUseSameServiceProvider(other)
                    && string.Equals(Extension.ConnectionString, otherBigQuery.Extension.ConnectionString, StringComparison.Ordinal);
            }
        }
    }
}