using Google.Cloud.BigQuery.V2;
using System.Data.Common;
using System.Data;
using Google.Apis.Bigquery.v2.Data;
using System.Globalization;
using System.Collections;
using System.Collections.ObjectModel;
using NetTopologySuite.Geometries;
using NetTopologySuite.IO;

namespace Ivy.Data.BigQuery
{
    public class BigQueryDataReader : DbDataReader, IDbColumnSchemaGenerator
    {
        private readonly BigQueryResults _results;
        private readonly BigQueryClient _client; //todo remove?
        private readonly TableSchema _schema;
        private readonly Dictionary<string, int> _fieldNameLookup;
        private readonly Type[] _fieldTypes;

        private IEnumerator<BigQueryRow>? _rowEnumerator;
        private BigQueryRow? _currentRow;
        private bool _isClosed;
        private bool _hasRows;
        private bool _readCalledOnce;
        private CommandBehavior _behavior;

        private readonly int _recordsAffected = -1;
        private readonly bool _closeConnection;
        private readonly BigQueryCommand _command;

        public BigQueryDataReader(BigQueryClient client, BigQueryResults results, BigQueryCommand command, CommandBehavior behavior, bool closeConnection, int recordsAffected = -1)
        {
            _client = client ?? throw new ArgumentNullException(nameof(client));
            _results = results ?? throw new ArgumentNullException(nameof(results));
            _recordsAffected = recordsAffected;
            _closeConnection = closeConnection;
            _command = command;
            _behavior = behavior;

            // For DML
            if (results.Schema == null)
            {
                _schema = new TableSchema();
                _fieldNameLookup = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                _fieldTypes = Array.Empty<Type>();
                _rowEnumerator = null;
                _hasRows = false;
                return;
            }

            _schema = results.Schema;
            _fieldNameLookup = new Dictionary<string, int>(_schema.Fields.Count, StringComparer.OrdinalIgnoreCase);
            _fieldTypes = new Type[_schema.Fields.Count];

            for (var i = 0; i < _schema.Fields.Count; i++)
            {
                var field = _schema.Fields[i];
                _fieldNameLookup[field.Name] = i;
                _fieldTypes[i] = TableFieldSchemaTypeToNetType(field.Type, field.Mode == "REPEATED");
            }

            _rowEnumerator = _results.GetEnumerator();

            if (_rowEnumerator == null) return;

            _hasRows = _rowEnumerator.MoveNext();

            if (_hasRows)
            {
                _currentRow = _rowEnumerator.Current;
                _rowEnumerator = _results.GetEnumerator();
                _currentRow = null;
            }

            else
            {
                _rowEnumerator.Dispose();
                _rowEnumerator = null;
            }
        }

        public override int FieldCount
        {
            get
            {
                EnsureNotClosed();
                return _isClosed || _recordsAffected > -1 ? 0 : _schema.Fields.Count;
            }
        }

        public override bool HasRows => !_isClosed && _hasRows;

        public override bool IsClosed => _isClosed;

        public override int RecordsAffected => _recordsAffected;

        public override int Depth => 0;

        public override object this[int ordinal] => GetValue(ordinal);

        public override object this[string name] => GetValue(GetOrdinal(name));

        public override void Close()
        {
            if (_isClosed) return;

            if (_closeConnection)
            {
                _command.Connection!.Close();
            }
            _currentRow = null;
            _rowEnumerator?.Dispose();
            _rowEnumerator = null;
            _isClosed = true;
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                Close();
            }
            base.Dispose(disposing);
        }

        public override bool Read()
        {
            EnsureNotClosed();

            if (_rowEnumerator == null)
            {
                return false;
            }

            if (_behavior == CommandBehavior.SingleRow && _readCalledOnce)
            {
                return false;
            }

            var hasMoreRows = _rowEnumerator.MoveNext();
            if (hasMoreRows)
            {
                _currentRow = _rowEnumerator.Current;
            }
            else
            {
                _currentRow = null;
                _rowEnumerator.Dispose();
                _rowEnumerator = null;
            }
            _readCalledOnce = true;
            return hasMoreRows;
        }

        public override DataTable GetSchemaTable()
        {
            EnsureNotClosed();

            if (RecordsAffected > 0 || _schema == null || FieldCount < 1)
            {
                return null;
            }

            var schemaTable = new DataTable("SchemaTable");
            schemaTable.Locale = CultureInfo.InvariantCulture;

            schemaTable.Columns.Add(SchemaTableColumn.ColumnName, typeof(string));
            schemaTable.Columns.Add(SchemaTableColumn.ColumnOrdinal, typeof(int));
            schemaTable.Columns.Add(SchemaTableColumn.ColumnSize, typeof(int));
            schemaTable.Columns.Add(SchemaTableColumn.NumericPrecision, typeof(short));
            schemaTable.Columns.Add(SchemaTableColumn.NumericScale, typeof(short));
            schemaTable.Columns.Add(SchemaTableColumn.DataType, typeof(Type));
            schemaTable.Columns.Add(SchemaTableOptionalColumn.ProviderSpecificDataType, typeof(BigQueryDbType));
            schemaTable.Columns.Add(SchemaTableColumn.IsLong, typeof(bool));
            schemaTable.Columns.Add(SchemaTableColumn.AllowDBNull, typeof(bool));
            schemaTable.Columns.Add(SchemaTableOptionalColumn.IsReadOnly, typeof(bool));
            schemaTable.Columns.Add(SchemaTableColumn.IsUnique, typeof(bool));
            schemaTable.Columns.Add(SchemaTableColumn.IsKey, typeof(bool));
            schemaTable.Columns.Add("BaseSchemaName", typeof(string));
            schemaTable.Columns.Add("BaseTableName", typeof(string));
            schemaTable.Columns.Add(SchemaTableColumn.BaseColumnName, typeof(string));

            for (var i = 0; i < _schema.Fields.Count; i++)
            {
                var field = _schema.Fields[i];
                var dataRow = schemaTable.NewRow();

                dataRow[SchemaTableColumn.ColumnName] = field.Name;
                dataRow[SchemaTableColumn.ColumnOrdinal] = i;
                dataRow[SchemaTableColumn.ColumnSize] = -1;
                dataRow[SchemaTableColumn.DataType] = _fieldTypes[i];
                dataRow[SchemaTableOptionalColumn.ProviderSpecificDataType] = field.Type;
                dataRow[SchemaTableColumn.IsLong] = false;
                dataRow[SchemaTableColumn.AllowDBNull] = field.Mode != "REQUIRED";
                dataRow[SchemaTableOptionalColumn.IsReadOnly] = true;
                dataRow[SchemaTableColumn.IsUnique] = false;
                dataRow[SchemaTableColumn.IsKey] = false;
                dataRow[SchemaTableColumn.BaseColumnName] = field.Name;

                switch (field.Type)
                {
                    case "NUMERIC":
                        dataRow[SchemaTableColumn.NumericPrecision] = (short)38;
                        dataRow[SchemaTableColumn.NumericScale] = (short)9;
                        break;
                    case "BIGNUMERIC":
                        dataRow[SchemaTableColumn.NumericPrecision] = (short)76;
                        dataRow[SchemaTableColumn.NumericScale] = (short)38;
                        break;
                    default:
                        dataRow[SchemaTableColumn.NumericPrecision] = DBNull.Value;
                        dataRow[SchemaTableColumn.NumericScale] = DBNull.Value;
                        break;
                }

                schemaTable.Rows.Add(dataRow);
            }
            return schemaTable;
        }

        public override string GetName(int ordinal)
        {
            EnsureNotClosed();
            if (FieldCount < 1)
            {
                throw new InvalidOperationException("There are no results");
            }
            ValidateOrdinal(ordinal);
            return _schema.Fields[ordinal].Name;
        }

        public override int GetOrdinal(string name)
        {
            EnsureNotClosed();
            if (_fieldNameLookup.TryGetValue(name, out var ordinal))
            {
                return ordinal;
            }
            throw new IndexOutOfRangeException($"Column '{name}' not found.");
        }

        public override Type GetFieldType(int ordinal)
        {
            EnsureNotClosed();
            if (FieldCount < 1)
            {
                throw new InvalidOperationException("There are no results");
            }
            ValidateOrdinal(ordinal);

            if (_currentRow?[ordinal] is string strGuid && Guid.TryParse(strGuid, out _))
            {
                return typeof(Guid);
            }

            return _fieldTypes[ordinal];
        }

        public override bool IsDBNull(int ordinal)
        {
            EnsureNotClosed();
            EnsureHasData();
            ValidateOrdinal(ordinal);
            return _currentRow?[ordinal] == null;
        }

        public override object GetValue(int ordinal)
        {
            EnsureNotClosed();
            EnsureHasData();
            ValidateOrdinal(ordinal);

            var value = _currentRow?[ordinal];

            //if (_schema.Fields[ordinal].Type == "BigQueryDbType.Timestamp" && value is DateTimeOffset dto)
            //{
            //    return dto.DateTime;
            //}
            // if (_schema.Fields[ordinal].Type == BigQueryDbType.Bytes && value is byte[] bytes)
            // {
            //     return bytes;
            // }

            return value ?? DBNull.Value;
        }

        public override bool GetBoolean(int ordinal) => GetFieldValue<bool>(ordinal);
        public override byte GetByte(int ordinal) => GetFieldValue<byte>(ordinal);
        public override decimal GetDecimal(int ordinal) => GetFieldValue<decimal>(ordinal);
        public override double GetDouble(int ordinal) => GetFieldValue<double>(ordinal);
        public override float GetFloat(int ordinal) => GetFieldValue<float>(ordinal);
        public override Guid GetGuid(int ordinal) => GetFieldValue<Guid>(ordinal);
        public override short GetInt16(int ordinal) => GetFieldValue<short>(ordinal);
        public override int GetInt32(int ordinal) => GetFieldValue<int>(ordinal);
        public override long GetInt64(int ordinal) => GetFieldValue<long>(ordinal);
        public override TextReader GetTextReader(int ordinal) => new StringReader(GetString(ordinal));

        public override DateTime GetDateTime(int ordinal)
        {
            EnsureNotClosed();
            EnsureHasData();
            ValidateOrdinal(ordinal);
            var value = (_currentRow?[ordinal]) ?? throw new InvalidCastException("Cannot cast DBNull.Value to DateTime.");
            var fieldType = Util.ParameterApiTypeToDbType(_schema.Fields[ordinal].Type);

            try
            {

                switch (fieldType)
                {
                    case BigQueryDbType.Timestamp:
                        if (value is DateTimeOffset dto) return dto.DateTime;
                        if (value is string sTimestamp) return DateTime.Parse(sTimestamp, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal);
                        break;

                    case BigQueryDbType.DateTime:
                        if (value is DateTime dt) return dt;
                        if (value is string sDateTime) return DateTime.Parse(sDateTime, CultureInfo.InvariantCulture);
                        break;

                    case BigQueryDbType.Date:
                        if (value is DateTime date) return date.Date;
                        if (value is string sDate) return DateTime.ParseExact(sDate, "yyyy-MM-dd", CultureInfo.InvariantCulture).Date;
                        break;
                }
            }
            catch (Exception ex) when (ex is FormatException || ex is InvalidCastException)
            {
                throw new InvalidCastException($"Cannot convert value for column {GetName(ordinal)} of type {value.GetType()} to DateTime.", ex);
            }

            return Convert.ToDateTime(value, CultureInfo.InvariantCulture);
        }


        public override char GetChar(int ordinal) => GetFieldValue<char>(ordinal);


        public override string GetString(int ordinal)
        {
            EnsureNotClosed();
            EnsureHasData();
            ValidateOrdinal(ordinal);
            var value = _currentRow?[ordinal];

            // Handle BigQueryGeography - return WKT text
            if (value is BigQueryGeography geography)
            {
                return geography.Text;
            }

            if (value is not string)
            {
                throw new InvalidCastException($"Cannot cast value of type '{value?.GetType()}' from column '{GetName(ordinal)}' to type 'System.String'.");
            }
            return (value == null ? null : Convert.ToString(value, CultureInfo.InvariantCulture)) ?? string.Empty;
        }


        public override T GetFieldValue<T>(int ordinal)
        {
            EnsureNotClosed();
            EnsureHasData();
            ValidateOrdinal(ordinal);

            var value = _currentRow?[ordinal];
            var bqFieldType = Util.ParameterApiTypeToDbType(_schema.Fields[ordinal].Type);

            if (value is null or DBNull)
            {
                if (default(T) == null && Nullable.GetUnderlyingType(typeof(T)) != null)
                {
                    return default;
                }
                throw new InvalidCastException($"Cannot cast DBNull.Value to type '{typeof(T)}'.");
            }

            if (typeof(T) == typeof(bool))
            {
                if (value is bool boolValue)
                    return (T)(object)boolValue;
                // Support reading bool from integer columns (0 = false, non-zero = true)
                if (value is long longValue)
                    return (T)(object)(longValue != 0);
                if (value is int intValue)
                    return (T)(object)(intValue != 0);
                throw new InvalidCastException($"Cannot cast value of type '{value?.GetType()}' from column '{GetName(ordinal)}' to type 'System.Boolean'.");
            }

            if (typeof(T) == typeof(Stream))
            {
                return (T)(object)GetStream(ordinal);
            }

            if (typeof(T) == typeof(TextReader) || typeof(T) == typeof(StringReader))
            {
                return (T)(object)GetTextReader(ordinal);
            }

            if (typeof(T) == typeof(Guid))
            {
                var stringValue = (string)GetValue(ordinal);
                if (string.IsNullOrWhiteSpace(stringValue))
                {
                    throw new InvalidCastException($"Cannot cast empty string to type 'System.Guid'.");
                }
                return (T)(object)Guid.Parse(stringValue);
            }

            if (typeof(T) == typeof(char))
            {
                var stringValue = (string)GetValue(ordinal);

                if (bqFieldType != BigQueryDbType.String || stringValue.Length > 1)
                {
                    throw new InvalidCastException($"Cannot cast value of type '{value?.GetType()}' from column '{GetName(ordinal)}' to type 'System.Char'.");
                }

                return stringValue.Length > 0 ? (T)(object)stringValue[0] : throw new InvalidCastException("Cannot cast empty string to type 'System.Char'.");
            }

            if (IsNumericType<T>() && !Util.IsNumericType(bqFieldType))
            {
                throw new InvalidCastException($"Cannot cast value of type '{value?.GetType()}' from column '{GetName(ordinal)}' to type '{typeof(T)}'.");
            }

            if (typeof(T) == typeof(string) && value is not string)
            {
                throw new InvalidCastException($"Cannot cast value of type '{value?.GetType()}' from column '{GetName(ordinal)}' to type '{typeof(T)}'.");
            }

            try
            {
                switch (value)
                {
                    case long longValue when typeof(T) == typeof(int):
                        return (T)(object)Convert.ToInt32(longValue);
                    case long longValue when typeof(T) == typeof(short):
                        return (T)(object)Convert.ToInt16(longValue);
                    case long longValue when typeof(T) == typeof(byte):
                        return (T)(object)Convert.ToByte(longValue);
                    //FLOAT64
                    case double doubleValue when typeof(T) == typeof(float):
                        return (T)(object)Convert.ToSingle(doubleValue);
                }

                switch (bqFieldType)
                {
                    case BigQueryDbType.Timestamp when value is DateTimeOffset dtoValue:
                        {
                            if (typeof(T) == typeof(DateTime)) return (T)(object)dtoValue.DateTime;
                            if (typeof(T) == typeof(DateTimeOffset)) return (T)(object)dtoValue;
                            break;
                        }
                    case BigQueryDbType.Date or BigQueryDbType.DateTime when value is DateTime dtValue:
                        {
                            if (typeof(T) == typeof(DateTime)) return (T)(object)dtValue;
                            if (typeof(T) == typeof(DateTimeOffset)) return (T)(object)new DateTimeOffset(dtValue, TimeSpan.Zero);
                            if (typeof(T) == typeof(DateOnly)) return (T)(object)DateOnly.FromDateTime(dtValue);
                            break;
                        }
                    case BigQueryDbType.Time when value is TimeSpan tsValue:
                        {
                            if (typeof(T) == typeof(TimeSpan)) return (T)(object)tsValue;
                            if (typeof(T) == typeof(TimeOnly)) return (T)(object)TimeOnly.FromTimeSpan(tsValue);
                            break;
                        }
                }

                if (value is DateTime dtValueFallback)
                {
                    if (typeof(T) == typeof(DateTimeOffset)) return (T)(object)new DateTimeOffset(dtValueFallback, TimeSpan.Zero);
                    if (typeof(T) == typeof(DateOnly)) return (T)(object)DateOnly.FromDateTime(dtValueFallback);
                    if (typeof(T) == typeof(TimeOnly)) return (T)(object)TimeOnly.FromDateTime(dtValueFallback);
                }
                if (value is TimeSpan tsValueFallback)
                {
                    if (typeof(T) == typeof(TimeOnly)) return (T)(object)TimeOnly.FromTimeSpan(tsValueFallback);
                }
                if (value is DateTimeOffset dtoValueFallback)
                {
                    if (typeof(T) == typeof(DateTime)) return (T)(object)dtoValueFallback.DateTime;
                }

                if (value is BigQueryNumeric numericValue)
                {
                    var targetType = Nullable.GetUnderlyingType(typeof(T)) ?? typeof(T);
                    if (targetType == typeof(BigQueryBigNumeric)) return (T)(object)BigQueryBigNumeric.Parse(numericValue.ToString());
                    if (targetType == typeof(decimal)) return (T)(object)numericValue.ToDecimal(LossOfPrecisionHandling.Truncate);
                    if (targetType == typeof(double)) return (T)(object)double.Parse(numericValue.ToString(), CultureInfo.InvariantCulture);
                    if (targetType == typeof(long)) return (T)(object)long.Parse(numericValue.ToString(), CultureInfo.InvariantCulture);
                    if (targetType == typeof(float)) return (T)(object)float.Parse(numericValue.ToString(), CultureInfo.InvariantCulture);
                    if (targetType == typeof(int)) return (T)(object)int.Parse(numericValue.ToString(), CultureInfo.InvariantCulture);
                    if (targetType == typeof(short)) return (T)(object)short.Parse(numericValue.ToString(), CultureInfo.InvariantCulture);
                    if (typeof(T) == typeof(string)) return (T)(object)numericValue.ToString();
                }

                if (value is BigQueryBigNumeric bigNumericValue)
                {
                    var targetType = Nullable.GetUnderlyingType(typeof(T)) ?? typeof(T);
                    if (targetType == typeof(BigQueryNumeric)) return (T)(object)BigQueryNumeric.Parse(bigNumericValue.ToString());
                    if (targetType == typeof(decimal)) return (T)(object)bigNumericValue.ToDecimal(LossOfPrecisionHandling.Truncate);
                    if (targetType == typeof(double)) return (T)(object)double.Parse(bigNumericValue.ToString(), CultureInfo.InvariantCulture);
                    if (targetType == typeof(long)) return (T)(object)long.Parse(bigNumericValue.ToString(), CultureInfo.InvariantCulture);
                    if (targetType == typeof(float)) return (T)(object)float.Parse(bigNumericValue.ToString(), CultureInfo.InvariantCulture);
                    if (targetType == typeof(int)) return (T)(object)int.Parse(bigNumericValue.ToString(), CultureInfo.InvariantCulture);
                    if (targetType == typeof(short)) return (T)(object)short.Parse(bigNumericValue.ToString(), CultureInfo.InvariantCulture);
                    if (typeof(T) == typeof(string)) return (T)(object)bigNumericValue.ToString();
                }

                if (bqFieldType == BigQueryDbType.Bytes && value is byte[] bytesValue && typeof(T) == typeof(byte[]))
                {
                    return (T)(object)bytesValue;
                }

                // GEOGRAPHY
                if (typeof(Geometry).IsAssignableFrom(typeof(T)))
                {
                    Geometry? geometry = null;

                    if (value is BigQueryGeography bqGeography)
                    {
                        // BigQueryGeography contains WKT in Text property
                        var wktReader = new WKTReader();
                        geometry = wktReader.Read(bqGeography.Text);
                    }
                    else if (value is string stringValue)
                    {
                        // Direct WKT string
                        var wktReader = new WKTReader();
                        geometry = wktReader.Read(stringValue);
                    }
                    else if (value is IDictionary<string, object> dict)
                    {
                        // Simple: {"Text":"POINT(0 0)","SRID":4326}
                        // Collection: {"Geometries":[{"Text":"LINESTRING(...)"},...],"SRID":4326}
                        geometry = ParseBigQueryGeography(dict);
                    }

                    if (geometry != null)
                    {
                        return (T)(object)geometry;
                    }

                    string valueJson;
                    try { valueJson = System.Text.Json.JsonSerializer.Serialize(value); }
                    catch { valueJson = value?.ToString() ?? "null"; }
                    throw new InvalidCastException($"Cannot convert geography value of type '{value.GetType()}' to '{typeof(T)}'. Value JSON: {valueJson}");
                }

                // ARRAY
                if (typeof(T).IsArray && value is Array sourceArray)
                {
                    var targetElementType = typeof(T).GetElementType()!;
                    var sourceElementType = value.GetType().GetElementType()!;

                    if (targetElementType == sourceElementType)
                    {
                        return (T)value;
                    }

                    // []
                    var targetArray = Array.CreateInstance(targetElementType, sourceArray.Length);
                    for (int i = 0; i < sourceArray.Length; i++)
                    {
                        var sourceElement = sourceArray.GetValue(i);
                        if (sourceElement != null)
                        {
                            var convertedElement = Convert.ChangeType(sourceElement, targetElementType, CultureInfo.InvariantCulture);
                            targetArray.SetValue(convertedElement, i);
                        }
                    }
                    return (T)(object)targetArray;
                }

                // List<T>
                if (typeof(T).IsGenericType && typeof(T).GetGenericTypeDefinition() == typeof(List<>) && value is Array listSourceArray)
                {
                    var targetElementType = typeof(T).GetGenericArguments()[0];
                    var listType = typeof(List<>).MakeGenericType(targetElementType);
                    var list = (IList)Activator.CreateInstance(listType)!;

                    foreach (var element in listSourceArray)
                    {
                        if (element != null)
                        {
                            var convertedElement = Convert.ChangeType(element, targetElementType, CultureInfo.InvariantCulture);
                            list.Add(convertedElement);
                        }
                        else
                        {
                            list.Add(null);
                        }
                    }
                    return (T)list;
                }

                switch (value)
                {
                    case T typedValue:
                        return typedValue;
                    case string s when string.IsNullOrWhiteSpace(s):
                        throw new InvalidCastException($"Cannot cast empty string to type '{typeof(T)}'.");
                }

                return (T)Convert.ChangeType(value, typeof(T), CultureInfo.InvariantCulture);
            }
            catch (InvalidCastException ice)
            {
                throw new InvalidCastException($"Cannot cast value of type '{value.GetType()}' from column '{GetName(ordinal)}' to type '{typeof(T)}'.", ice);
            }
            catch (OverflowException oe)
            {
                throw new InvalidCastException($"Value '{value}' from column '{GetName(ordinal)}' is out of range for type '{typeof(T)}'.", oe);
            }
        }

        public override int GetValues(object[] values)
        {
            EnsureNotClosed();
            EnsureHasData();

            if (values == null) throw new ArgumentNullException(nameof(values));

            var count = Math.Min(values.Length, FieldCount);
            for (var i = 0; i < count; i++)
            {
                values[i] = GetValue(i);
            }
            return count;
        }

        public override IEnumerator GetEnumerator()
        {
            return new DbEnumerator(this, closeReader: false);
        }

        public override Type GetProviderSpecificFieldType(int ordinal)
        {
            EnsureNotClosed();
            ValidateOrdinal(ordinal);
            var value = _currentRow?[ordinal];

            if (value != null) return value.GetType();

            var field = _schema.Fields[ordinal];
            var isArray = field.Mode == "REPEATED";

            var type = field.Type;

            return TableFieldSchemaTypeToNetType(type, isArray);
        }

        public override Stream GetStream(int ordinal)
        {
            EnsureNotClosed();
            EnsureHasData();
            ValidateOrdinal(ordinal);

            var bqTypename = GetDataTypeName(ordinal);
            var bqType = Util.ParameterApiTypeToDbType(bqTypename);
            if (bqType != BigQueryDbType.Bytes)
            {
                throw new InvalidCastException($"Cannot get Stream for column '{GetName(ordinal)}' with BigQuery type '{bqType}'. Stream is only supported for BYTES type.");
            }

            var value = _currentRow[ordinal];

            if (value == null)
            {
                throw new InvalidCastException($"Cannot get Stream for column '{GetName(ordinal)}' because the value is DBNull.");
            }

            try
            {
                var bytes = (byte[])value;
                return new MemoryStream(bytes, writable: false);
            }
            catch (InvalidCastException ice)
            {
                throw new InvalidCastException($"Underlying value type mismatch for BYTES column '{GetName(ordinal)}'. Expected System.Byte[] but received '{value?.GetType()}'.", ice);
            }
        }

        public override object GetProviderSpecificValue(int ordinal)
        {
            EnsureNotClosed();
            EnsureHasData();
            ValidateOrdinal(ordinal);
            return _currentRow[ordinal];
        }

        public override int GetProviderSpecificValues(object[] values)
        {
            EnsureNotClosed();
            EnsureHasData();

            if (values == null)
            {
                throw new ArgumentNullException(nameof(values));
            }

            var count = Math.Min(values.Length, FieldCount);
            for (var i = 0; i < count; i++)
            {
                values[i] = GetProviderSpecificValue(i);
            }
            return count;
        }

        public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
        {
            EnsureNotClosed();
            EnsureHasData();
            ValidateOrdinal(ordinal);

            ArgumentOutOfRangeException.ThrowIfNegative(dataOffset);
            ArgumentOutOfRangeException.ThrowIfGreaterThan(dataOffset, int.MaxValue);

            var value = _currentRow?[ordinal];
            if (value == null || value == DBNull.Value)
            {
                return 0;
            }

            if (value is not byte[] sourceBuffer)
            {
                throw new InvalidCastException($"Column '{GetName(ordinal)}' is not a byte array (BYTES type).");
            }

            if (buffer == null)
            {
                return sourceBuffer.Length;
            }

            if (bufferOffset < 0 || bufferOffset >= buffer.Length + 1)
                throw new ArgumentException($"bufferOffset must be between 0 and {buffer.Length}");
            if (length > buffer.Length - bufferOffset)
                throw new IndexOutOfRangeException($"length must be between 0 and {buffer.Length - bufferOffset}");

            dataOffset = Math.Max(0, Math.Min(dataOffset, sourceBuffer.Length));

            var bytesAvailable = sourceBuffer.Length - dataOffset;
            var bytesToCopy = (int)Math.Min(length, bytesAvailable);
            bytesToCopy = Math.Min(bytesToCopy, buffer.Length - bufferOffset);

            if (bytesToCopy > 0)
            {
                Buffer.BlockCopy(sourceBuffer, (int)dataOffset, buffer, bufferOffset, bytesToCopy);
            }
            else
            {
                bytesToCopy = 0;
            }

            return bytesToCopy;
        }

        public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
        {
            EnsureNotClosed();
            EnsureHasData();
            ValidateOrdinal(ordinal);

            //Todo refactor
            ArgumentOutOfRangeException.ThrowIfNegative(dataOffset);
            ArgumentOutOfRangeException.ThrowIfGreaterThan(dataOffset, int.MaxValue);
            if (bufferOffset < 0 || bufferOffset >= buffer?.Length + 1)
                throw new ArgumentOutOfRangeException($"bufferOffset must be between 0 and {buffer?.Length}");
            if (checked(bufferOffset + length) > buffer.Length)
            {
                throw new ArgumentException($"bufferOffset + length can't be bigger than {buffer.Length} (buffer.Length)");
            }

            var value = GetString(ordinal);

            if (string.IsNullOrEmpty(value))
            {
                return 0;
            }

            var sourceBuffer = value.ToCharArray();

            if (buffer == null)
            {
                return sourceBuffer.Length;
            }

            dataOffset = Math.Max(0, Math.Min(dataOffset, sourceBuffer.Length));
            var charsAvailable = sourceBuffer.Length - dataOffset;
            var charsToCopy = (int)Math.Min(length, charsAvailable);
            charsToCopy = Math.Min(charsToCopy, buffer.Length - bufferOffset);

            if (charsToCopy > 0)
            {
                Array.Copy(sourceBuffer, (int)dataOffset, buffer, bufferOffset, charsToCopy);
            }
            else
            {
                charsToCopy = 0;
            }

            return charsToCopy;
        }

        public override string GetDataTypeName(int ordinal)
        {
            EnsureNotClosed();
            //ValidateOrdinal(ordinal);
            if (!HasRows)
            {
                throw new InvalidOperationException("There are no results");
            }

            var field = _schema.Fields[ordinal];

            var baseTypeName = field.Type;

            if (field.Mode == "REPEATED")
            {
                return baseTypeName + "[]";
            }

            return baseTypeName;
        }

        // Client doesn't support multiple result sets (only returns the first one). 
        public override bool NextResult()
        {
            EnsureNotClosed();
            _rowEnumerator?.Dispose();
            _rowEnumerator = null;
            _currentRow = null;
            _hasRows = false;
            return false;
        }

        public ReadOnlyCollection<DbColumn> GetColumnSchema()
        {
            EnsureNotClosed();

            if (_schema?.Fields == null || _schema.Fields.Count == 0)
            {
                return new ReadOnlyCollection<DbColumn>(new List<DbColumn>());
            }

            var columnSchemaList = new List<DbColumn>(FieldCount);

            for (var i = 0; i < FieldCount; i++)
            {
                var field = _schema.Fields[i];
                var bqDbType = field.Type;
                var netType = _fieldTypes[i];
                var bqTypeName = GetDataTypeName(i);

                int? columnSize = -1;
                int? numericPrecision = null;
                int? numericScale = null;
                var allowDbNull = field.Mode != "REQUIRED";

                if (Util.ParameterApiTypeToDbType(bqDbType) == BigQueryDbType.Numeric)
                {
                    numericPrecision = 38;
                    numericScale = 9;
                }
                else if (Util.ParameterApiTypeToDbType(bqDbType) == BigQueryDbType.BigNumeric)
                {
                    numericPrecision = 77;
                    numericScale = 38;
                }

                var dbColumn = new BigQueryDbColumn(
                    columnName: field.Name,
                    ordinal: i,
                    dataType: netType,
                    dataTypeName: bqTypeName,
                    allowDbNull: allowDbNull,
                    columnSize: columnSize,
                    numericPrecision: numericPrecision,
                    numericScale: numericScale
                );

                columnSchemaList.Add(dbColumn);
            }

            return new ReadOnlyCollection<DbColumn>(columnSchemaList);
        }

        private void EnsureNotClosed()
        {
            if (_isClosed)
            {
                throw new InvalidOperationException("Cannot perform operation on a closed DataReader.");
            }
        }

        private void ValidateOrdinal(int ordinal)
        {
            if (ordinal < 0 || ordinal >= FieldCount)
            {
                throw new IndexOutOfRangeException($"Ordinal must be between 0 and {FieldCount - 1}.");
            }
        }

        private void EnsureHasData()
        {
            if (_currentRow != null) return;

            if (!_readCalledOnce)
            {
                throw new InvalidOperationException("No data exists for the row. Call Read() first.");
            }
            else
            {
                throw new InvalidOperationException("No data exists for the row. Invalid attempt to read data when reader is positioned before the first row or after the last row.");
            }
        }

        bool IsNumericType<T>()
        {
            var type = typeof(T);
            return type == typeof(byte) || type == typeof(sbyte) ||
                   type == typeof(ushort) || type == typeof(uint) || type == typeof(ulong) ||
                   type == typeof(short) || type == typeof(int) || type == typeof(long) ||
                   type == typeof(float) || type == typeof(double) || type == typeof(decimal);
        }

        private static Type TableFieldSchemaTypeToNetType(string type, bool isArray)
        {
            var baseType = type switch
            {
                "INT64" or "INTEGER" => isArray ? typeof(long[]) : typeof(long),
                "FLOAT64" => isArray ? typeof(double[]) : typeof(double),
                "BOOL" => isArray ? typeof(bool[]) : typeof(bool),
                "STRING" => isArray ? typeof(string[]) : typeof(string),
                "BYTES" => isArray ? typeof(byte[][]) : typeof(byte[]),
                "TIMESTAMP" => isArray ? typeof(DateTimeOffset[]) : typeof(DateTimeOffset?),
                "DATE" => isArray ? typeof(DateTime[]) : typeof(DateTime?),
                "TIME" => isArray ? typeof(TimeSpan[]) : typeof(TimeSpan?),
                "DATETIME" => isArray ? typeof(DateTime[]) : typeof(DateTime),
                "NUMERIC" => isArray ? typeof(BigQueryNumeric[]) : typeof(BigQueryNumeric),
                "BIGNUMERIC" => isArray ? typeof(BigQueryBigNumeric[]) : typeof(BigQueryBigNumeric),
                "GEOGRAPHY" => isArray ? typeof(string[]) : typeof(string),
                "JSON" => isArray ? typeof(string[]) : typeof(string),
                "STRUCT" => isArray ? typeof(Dictionary<string, object>[]) : typeof(Dictionary<string, object>),
                "ARRAY" => typeof(object[]),
                _ => typeof(object),
            };

            //return isArray ? typeof(object[]) : typeof(object);
            return isArray ? Array.CreateInstance(baseType, 0).GetType() : baseType;
        }

        /// <summary>
        /// Parses BigQuery's custom geography dictionary format into NTS Geometry.
        /// BigQuery format:
        /// - Simple: {"Text":"POINT(0 0)","SRID":4326}
        /// - Collection: {"Geometries":[{"Text":"LINESTRING(...)"},...],"SRID":4326}
        /// </summary>
        private static Geometry? ParseBigQueryGeography(IDictionary<string, object> dict)
        {
            var wktReader = new WKTReader();

            if (dict.TryGetValue("Text", out var textObj) && textObj is string wkt)
            {
                var geometry = wktReader.Read(wkt);

                if (dict.TryGetValue("SRID", out var sridObj))
                {
                    var srid = Convert.ToInt32(sridObj);
                    geometry.SRID = srid;
                }

                return geometry;
            }

            if (dict.TryGetValue("Geometries", out var geometriesObj))
            {
                IEnumerable<object>? geometriesEnumerable = null;

                if (geometriesObj is object[] arr)
                    geometriesEnumerable = arr;
                else if (geometriesObj is IList list)
                    geometriesEnumerable = list.Cast<object>();
                else if (geometriesObj is IEnumerable enumerable && geometriesObj is not string)
                    geometriesEnumerable = enumerable.Cast<object>();

                if (geometriesEnumerable == null)
                {
                    throw new InvalidCastException($"Geometries property has unexpected type: {geometriesObj?.GetType().FullName}");
                }

                var geometriesArray = geometriesEnumerable.ToArray();
                var geometries = new List<Geometry>();

                foreach (var item in geometriesArray)
                {
                    if (item is IDictionary<string, object> geomDict)
                    {
                        var geom = ParseBigQueryGeography(geomDict);
                        if (geom != null)
                        {
                            geometries.Add(geom);
                        }
                    }
                    else if (item is BigQueryGeography bqGeom)
                    {
                        var geom = wktReader.Read(bqGeom.Text);
                        geometries.Add(geom);
                    }
                    else
                    {
                        throw new InvalidCastException($"Geometry item has unexpected type: {item?.GetType().FullName}. Value: {item}");
                    }
                }

                if (geometries.Count == 0)
                {
                    throw new InvalidCastException($"No geometries parsed from array of {geometriesArray.Length} items");
                }

                var srid = 0;
                if (dict.TryGetValue("SRID", out var collectionSridObj))
                {
                    srid = Convert.ToInt32(collectionSridObj);
                }

                var factory = new GeometryFactory(new PrecisionModel(), srid);

                var firstGeom = geometries[0];
                if (geometries.All(g => g is Point))
                {
                    return factory.CreateMultiPoint(geometries.Cast<Point>().ToArray());
                }
                if (geometries.All(g => g is LineString))
                {
                    return factory.CreateMultiLineString(geometries.Cast<LineString>().ToArray());
                }
                if (geometries.All(g => g is Polygon))
                {
                    return factory.CreateMultiPolygon(geometries.Cast<Polygon>().ToArray());
                }

                // Mixed types
                return factory.CreateGeometryCollection(geometries.ToArray());
            }

            return null;
        }
    }
}