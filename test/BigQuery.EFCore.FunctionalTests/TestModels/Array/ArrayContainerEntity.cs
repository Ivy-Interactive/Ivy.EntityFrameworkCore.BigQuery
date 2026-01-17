namespace Ivy.EntityFrameworkCore.BigQuery.TestModels.Array;

public class ArrayContainerEntity
{
    public int Id { get; set; }
    public List<ArrayEntity> ArrayEntities { get; set; } = null!;
}
