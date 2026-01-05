using Ivy.EFCore.BigQuery.FunctionalTests.TestModels.BigQueryArray;
using Ivy.EntityFrameworkCore.BigQuery.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace Ivy.EFCore.BigQuery.FunctionalTests.Query;

public abstract class BigQueryArrayQueryFixture : SharedStoreFixtureBase<BigQueryArrayContext>, IQueryFixtureBase, ITestSqlLoggerFactory
{
    protected override ITestStoreFactory TestStoreFactory
        => BigQueryTestStoreFactory.Instance;

    public TestSqlLoggerFactory TestSqlLoggerFactory
        => (TestSqlLoggerFactory)ListLoggerFactory;

    private BigQueryArrayData? _expectedData;

    public override DbContextOptionsBuilder AddOptions(DbContextOptionsBuilder builder)
        => base.AddOptions(builder)
            .ConfigureWarnings(wcb => wcb.Ignore(CoreEventId.CollectionWithoutComparer));

    protected override Task SeedAsync(BigQueryArrayContext context)
        => BigQueryArrayContext.SeedAsync(context);

    public Func<DbContext> GetContextCreator()
        => CreateContext;

    public ISetSource GetExpectedData()
        => _expectedData ??= new BigQueryArrayData();

    public IReadOnlyDictionary<Type, object> EntitySorters
        => new Dictionary<Type, Func<object, object>>
        {
            { typeof(BigQueryArrayEntity), e => ((BigQueryArrayEntity)e)?.Id ?? 0 },
            { typeof(BigQueryArrayContainerEntity), e => ((BigQueryArrayContainerEntity)e)?.Id ?? 0 }
        }.ToDictionary(e => e.Key, e => (object)e.Value);

    public IReadOnlyDictionary<Type, object> EntityAsserters
        => new Dictionary<Type, Action<object?, object?>>
        {
            {
                typeof(BigQueryArrayEntity), (e, a) =>
                {
                    Assert.Equal(e is null, a is null);
                    if (a is not null)
                    {
                        var expected = (BigQueryArrayEntity)e!;
                        var actual = (BigQueryArrayEntity)a;

                        Assert.Equal(expected.Id, actual.Id);
                        Assert.Equal(expected.IntArray, actual.IntArray);
                        Assert.Equal(expected.IntList, actual.IntList);
                        Assert.Equal(expected.LongArray, actual.LongArray);
                        Assert.Equal(expected.StringArray, actual.StringArray);
                        Assert.Equal(expected.StringList, actual.StringList);
                        Assert.Equal(expected.DoubleArray, actual.DoubleArray);
                        Assert.Equal(expected.DoubleList, actual.DoubleList);
                        Assert.Equal(expected.BoolArray, actual.BoolArray);
                        Assert.Equal(expected.ByteArray, actual.ByteArray);
                        Assert.Equal(expected.Name, actual.Name);
                        Assert.Equal(expected.Score, actual.Score);
                    }
                }
            },
            {
                typeof(BigQueryArrayContainerEntity), (e, a) =>
                {
                    Assert.Equal(e is null, a is null);
                    if (a is not null)
                    {
                        var expected = (BigQueryArrayContainerEntity)e!;
                        var actual = (BigQueryArrayContainerEntity)a;

                        Assert.Equal(expected.Id, actual.Id);
                        Assert.Equal(expected.ArrayEntities.Count, actual.ArrayEntities.Count);
                    }
                }
            }
        }.ToDictionary(e => e.Key, e => (object)e.Value);
}

public class BigQueryArrayArrayQueryFixture : BigQueryArrayQueryFixture
{
    protected override string StoreName => "BigQueryArrayTests";
}
