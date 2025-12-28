using Microsoft.EntityFrameworkCore.TestUtilities;

namespace Ivy.EntityFrameworkCore.BigQuery.TestUtilities
{
    public class BigQueryUpdateTestStoreFactory : BigQueryTestStoreFactory
    {
        public const string Name = "BigQueryUpdateTest";

        public new static BigQueryUpdateTestStoreFactory Instance { get; } = new();

        protected BigQueryUpdateTestStoreFactory()
        {
        }

        public override TestStore GetOrCreate(string storeName)
            => BigQueryTestStore.GetOrCreate(storeName);
    }
}
