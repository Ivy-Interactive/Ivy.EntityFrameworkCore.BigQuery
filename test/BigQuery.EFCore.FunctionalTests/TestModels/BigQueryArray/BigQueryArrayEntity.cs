namespace Ivy.EFCore.BigQuery.FunctionalTests.TestModels.BigQueryArray;

public class BigQueryArrayEntity
{
    public int Id { get; set; }

    public int[] IntArray { get; set; } = null!;
    public List<int> IntList { get; set; } = null!;
    public long[] LongArray { get; set; } = null!;

    public string[] StringArray { get; set; } = null!;
    public List<string> StringList { get; set; } = null!;

    public double[] DoubleArray { get; set; } = null!;
    public List<double> DoubleList { get; set; } = null!;

    public bool[] BoolArray { get; set; } = null!;

    public byte[] ByteArray { get; set; } = null!;

    public string Name { get; set; } = null!;
    public int Score { get; set; }
}
