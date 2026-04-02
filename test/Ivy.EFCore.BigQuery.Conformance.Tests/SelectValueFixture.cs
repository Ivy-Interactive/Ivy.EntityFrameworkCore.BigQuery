using System.Collections.ObjectModel;
using System.Data;
using AdoNet.Specification.Tests;

namespace Ivy.Data.BigQuery.Conformance.Tests;

public class SelectValueFixture : DbFactoryFixture, ISelectValueFixture, IDeleteFixture
{
    private const string DefaultDatasetId = "ado_tests";
    private const string TableName = "select_value";

    public SelectValueFixture() => CreateSelectValueTable(this);

    public string CreateSelectSql(DbType dbType, ValueKind kind)
    {
        return $"SELECT `{dbType.ToString()}` from {DefaultDatasetId}.{TableName} WHERE Id = {(int)kind};";
    }

    public string CreateSelectSql(byte[] value)
    {
        return $"SELECT FROM_BASE64('{Convert.ToBase64String(value)}') AS Value";
    }

    public string SelectNoRows => $"SELECT * FROM `{DefaultDatasetId}.{TableName}` WHERE false;";

    public string DeleteNoRows => $"DELETE FROM {DefaultDatasetId}.{TableName} WHERE false;";

    public Type NullValueExceptionType => typeof(InvalidCastException);

    public IReadOnlyCollection<DbType> SupportedDbTypes { get; } = new ReadOnlyCollection<DbType>([
        DbType.Int64,
        DbType.Binary,  //BYTES
        DbType.Boolean, //BOOL
        DbType.Byte,    // INT64
        DbType.SByte,   // INT64
        DbType.Int16,   // INT64
        //DbType.UInt16,   // INT64
        DbType.Int32,   // INT64
        DbType.Int64,   // INT64
        //DbType.UInt16,   // INT64
        
        DbType.Double,
        DbType.Single, // FLOAT64
        
        DbType.String,
        DbType.AnsiString,
        DbType.StringFixedLength,
        DbType.AnsiStringFixedLength,
        DbType.Binary,
        DbType.Guid, // Tested as STRING
        DbType.Date,
        DbType.Time,
        DbType.DateTime,
        DbType.DateTime2, // DATETIME
        //DbType.DateTimeOffset, //Todo TIMESTAMP UTC
        DbType.Decimal, // NUMERIC
        DbType.VarNumeric // BIGNUMERIC
    ]);

    public void DropSelectValueTable(IDbFactoryFixture factoryFixture) => Util.ExecuteNonQuery(factoryFixture, $"DROP TABLE IF EXISTS `{DefaultDatasetId}.{TableName}`;");

    public void CreateSelectValueTable(IDbFactoryFixture factoryFixture)
    {
        DropSelectValueTable(factoryFixture);

        Util.ExecuteNonQuery(factoryFixture, $"""

                                              CREATE TABLE `{DefaultDatasetId}.{TableName}` (
                                                Id INT64 NOT NULL,
                                                `Binary` BYTES,
                                                Boolean BOOL,
                                                Byte INT64,
                                                SByte INT64,
                                                Int16 INT64,
                                                UInt16 INT64,
                                                Int32 INT64,
                                                Int64 INT64,
                                                UInt64 BIGNUMERIC,
                                                Single FLOAT64,
                                                `Double` FLOAT64,
                                                `Decimal` BIGNUMERIC(56,28),
                                                String STRING,
                                                Guid STRING,
                                                `Date` DATE,
                                                `DateTime` DATETIME,
                                                `Time` TIME
                                              );

                                              """);

        Util.ExecuteNonQuery(factoryFixture, $"""

                                              INSERT INTO `{DefaultDatasetId}.{TableName}` (
                                                Id, `Binary`, Boolean, Byte, SByte, Int16, UInt16, Int32, Int64, UInt64,
                                                Single, `Double`, `Decimal`, String, Guid, `Date`, `DateTime`, `Time`
                                              )
                                              VALUES
                                                (0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
                                                (1, b'', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '', '', NULL, NULL, NULL),
                                                (2, b'\x00', FALSE, 0, 0, 0, 0, 0, 0, 0, 0.0, 0.0, 0.0000000000000000000000000000, '0', '00000000-0000-0000-0000-000000000000', NULL, NULL, TIME '00:00:00'),
                                                (3, b'\x11', TRUE, 1, 1, 1, 1, 1, 1, 1, 1.0, 1.0, 1, '1', '11111111-1111-1111-1111-111111111111', DATE '1111-11-11', DATETIME '1111-11-11 11:11:11.111', TIME '11:11:11.111'),
                                                (4, NULL, FALSE, 0, -128, -32768, 0, -2147483648, -9223372036854775808, 0, 1.18e-38, 2.23e-308, BIGNUMERIC '0.000000000000001', NULL, '33221100-5544-7766-9988-aabbccddeeff', DATE '0001-01-01', DATETIME '0001-01-01 00:00:00', TIME '00:00:00'),
                                                (5, NULL, TRUE, 255, 127, 32767, 65535, 2147483647, 9223372036854775807, BIGNUMERIC '18446744073709551615', 3.40e+38, 1.79e+308, BIGNUMERIC '99999999999999999999.999999999999999', NULL, 'ccddeeff-aabb-8899-7766-554433221100', DATE '9999-12-31', DATETIME '9999-12-31 23:59:59.999', TIME '23:59:59.999999');        

                                              """);
    }

    //public void Dispose() => Util.ExecuteNonQuery(this, $"DROP TABLE IF EXISTS `{DefaultDatasetId}.{TableName}`;");
}