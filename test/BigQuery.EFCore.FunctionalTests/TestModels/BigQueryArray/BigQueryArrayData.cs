using Microsoft.EntityFrameworkCore.TestUtilities;

namespace Ivy.EFCore.BigQuery.FunctionalTests.TestModels.BigQueryArray;

public class BigQueryArrayData : ISetSource
{
    public IReadOnlyList<BigQueryArrayEntity> ArrayEntities { get; } = CreateArrayEntities();
    public IReadOnlyList<BigQueryArrayContainerEntity> ContainerEntities { get; } = CreateContainerEntities();

    public IQueryable<TEntity> Set<TEntity>()
        where TEntity : class
    {
        if (typeof(TEntity) == typeof(BigQueryArrayEntity))
        {
            return (IQueryable<TEntity>)ArrayEntities.AsQueryable();
        }

        if (typeof(TEntity) == typeof(BigQueryArrayContainerEntity))
        {
            return (IQueryable<TEntity>)ContainerEntities.AsQueryable();
        }

        throw new InvalidOperationException("Invalid entity type: " + typeof(TEntity));
    }

    public static IReadOnlyList<BigQueryArrayEntity> CreateArrayEntities()
        =>
        [
            new()
            {
                Id = 1,
                IntArray = [1, 2, 3],
                IntList = [1, 2, 3],
                LongArray = [10L, 20L, 30L],
                StringArray = ["apple", "banana", "cherry"],
                StringList = ["apple", "banana", "cherry"],
                DoubleArray = [1.1, 2.2, 3.3],
                DoubleList = [1.1, 2.2, 3.3],
                BoolArray = [true, false, true],
                ByteArray = [1, 2, 3],
                Name = "First",
                Score = 100
            },
            new()
            {
                Id = 2,
                IntArray = [4, 5, 6, 7],
                IntList = [4, 5, 6, 7],
                LongArray = [40L, 50L, 60L, 70L],
                StringArray = ["dog", "elephant", "fox", "giraffe"],
                StringList = ["dog", "elephant", "fox", "giraffe"],
                DoubleArray = [4.4, 5.5, 6.6, 7.7],
                DoubleList = [4.4, 5.5, 6.6, 7.7],
                BoolArray = [false, false, true, true],
                ByteArray = [4, 5, 6, 7],
                Name = "Second",
                Score = 200
            },
            new()
            {
                Id = 3,
                IntArray = [10, 11, 12],
                IntList = [10, 11, 12],
                LongArray = [100L, 110L, 120L],
                StringArray = ["hello", "world", "test"],
                StringList = ["hello", "world", "test"],
                DoubleArray = [10.0, 11.1, 12.2],
                DoubleList = [10.0, 11.1, 12.2],
                BoolArray = [true, true, false],
                ByteArray = [10, 11, 12],
                Name = "Third",
                Score = 50
            }
        ];

    public static IReadOnlyList<BigQueryArrayContainerEntity> CreateContainerEntities()
        => [new BigQueryArrayContainerEntity { Id = 1, ArrayEntities = CreateArrayEntities().ToList() }];
}
