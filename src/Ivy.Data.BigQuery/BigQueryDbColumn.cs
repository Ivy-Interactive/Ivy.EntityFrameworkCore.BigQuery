using Google.Cloud.BigQuery.V2;
using System.Data.Common;

namespace Ivy.Data.BigQuery;

public class BigQueryDbColumn : DbColumn
{
    public BigQueryDbColumn(
        string columnName,
        int ordinal,
        Type dataType,
        string dataTypeName,
        bool allowDbNull,
        int? columnSize,
        int? numericPrecision,
        int? numericScale,
        bool isReadOnly = true,
        bool isUnique = false,
        bool isKey = false,
        bool isLong = false,
        string? baseSchemaName = null,
        string? baseTableName = null
        )
    {
        ColumnName = columnName;
        ColumnOrdinal = ordinal;
        DataType = dataType;
        DataTypeName = dataTypeName;
        AllowDBNull = allowDbNull;
        ColumnSize = columnSize;
        NumericPrecision = numericPrecision;
        NumericScale = numericScale;
        IsReadOnly = isReadOnly;
        IsUnique = isUnique;
        IsKey = isKey;
        IsLong = isLong;
        BaseSchemaName = baseSchemaName;
        BaseTableName = baseTableName;
        BaseColumnName = columnName;

        IsAliased = false;
        IsExpression = false;
        IsHidden = false;
        IsIdentity = false;
        IsAutoIncrement = false;
        UdtAssemblyQualifiedName = null;
        BaseCatalogName = null;
        BaseServerName = null;

    }
}