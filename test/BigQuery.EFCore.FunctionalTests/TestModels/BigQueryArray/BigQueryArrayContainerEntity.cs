namespace Ivy.EFCore.BigQuery.FunctionalTests.TestModels.BigQueryArray;

public class BigQueryArrayContainerEntity
{
    public int Id { get; set; }
    public List<BigQueryArrayEntity> ArrayEntities { get; set; } = null!;
}
