using Microsoft.EntityFrameworkCore.Update;

namespace Ivy.EntityFrameworkCore.BigQuery.Update.Internal
{
    public class BigQueryModificationCommandBatchFactory : IModificationCommandBatchFactory
    {
        private readonly ModificationCommandBatchFactoryDependencies _dependencies;

        public BigQueryModificationCommandBatchFactory(
            ModificationCommandBatchFactoryDependencies dependencies)
        {
            _dependencies = dependencies;
        }

        public virtual ModificationCommandBatch Create()
            => new BigQueryModificationCommandBatch(_dependencies);
    }
}