using System.Collections;
using Google.Cloud.BigQuery.V2;
using System.Data.Common;

namespace Ivy.Data.BigQuery;

public class BigQueryParameterCollection : DbParameterCollection
{
    private readonly List<BigQueryParameter> _parameters = new List<BigQueryParameter>();
    private readonly object _syncRoot = new object();

    public override int Count => _parameters.Count;
    public override object SyncRoot => _syncRoot;

    public BigQueryParameter this[string parameterName]
    {
        get => (BigQueryParameter)GetParameter(parameterName);
        set => SetParameter(parameterName, value);
    }

    public BigQueryParameter this[int index]
    {
        get => (BigQueryParameter)GetParameter(index);
        set => SetParameter(index, value);
    }

    public BigQueryParameter Add(BigQueryParameter value)
    {
        ArgumentNullException.ThrowIfNull(value);

        if (Contains(value.ParameterName))
        {
            return (BigQueryParameter)GetParameter(value.ParameterName);
        }
        
        if (value.Collection is not null)
        {
            var copy = new BigQueryParameter
            {
                ParameterName = value.ParameterName,
                Value = value.Value,
                Direction = value.Direction,
                IsNullable = value.IsNullable,
                Size = value.Size,
                DbType = value.DbType
            };
            if (value.BigQueryDbType.HasValue)
            {
                copy.BigQueryDbType = value.BigQueryDbType.Value;
            }
            value = copy;
        }

        _parameters.Add(value);
        value.Collection = this;

        return value;
    }

    public override int Add(object value)
    {
        ArgumentNullException.ThrowIfNull(value);
        if (value is not BigQueryParameter parameter)
        {
            throw new ArgumentException($"Value must be of type {nameof(BigQueryParameter)}.", nameof(value));
        }

        Add(parameter);
        return Count - 1;
    }

    public BigQueryParameter AddWithValue(string parameterName, object value)
    {
        return Add(new BigQueryParameter(parameterName, value));
    }

    public BigQueryParameter Add(string parameterName, BigQueryDbType bqDbType)
    {
        return Add(new BigQueryParameter(parameterName, bqDbType));
    }

    public BigQueryParameter Add(string parameterName, BigQueryDbType bqDbType, object value)
    {
        return Add(new BigQueryParameter(parameterName, bqDbType, value));
    }

    public override void AddRange(Array values)
    {
        if (values == null) throw new ArgumentNullException(nameof(values));

        foreach (var item in values)
        {
            Add(item);
        }
    }

    public void AddRange(IEnumerable<BigQueryParameter> values)
    {
        if (values == null) throw new ArgumentNullException(nameof(values));

        foreach (var parameter in values)
        {
            Add(parameter);
        }
    }

    public override void Clear() => _parameters.Clear();

    public override bool Contains(object value)
    {
        return value is BigQueryParameter parameter && _parameters.Contains(parameter);
    }

    public override bool Contains(string value) => IndexOf(value) != -1;

    public override int IndexOf(object value)
    {
        if (value is not BigQueryParameter parameter) return -1;
        return _parameters.IndexOf(parameter);
    }

    public override int IndexOf(string parameterName)
    {
        if (parameterName is null)
            return -1;

        if (parameterName.Length > 0 && (parameterName[0] == ':' || parameterName[0] == '@'))
            parameterName = parameterName.Remove(0, 1);

        for (var i = 0; i < _parameters.Count; i++)
        {
            if (string.Equals(_parameters[i].ParameterName, parameterName, StringComparison.OrdinalIgnoreCase))
            {
                return i;
            }
        }
        return -1;
    }

    public override void Insert(int index, object value)
    {
        ArgumentNullException.ThrowIfNull(value);

        if (value is not BigQueryParameter parameter)
        {
            throw new ArgumentException($"Value must be of type {nameof(BigQueryParameter)}.", nameof(value));
        }

        if (Contains(parameter.ParameterName))
        {
            return;
        }
        
        if (parameter.Collection != null)
        {
            var copy = new BigQueryParameter
            {
                ParameterName = parameter.ParameterName,
                Value = parameter.Value,
                Direction = parameter.Direction,
                IsNullable = parameter.IsNullable,
                Size = parameter.Size,
                DbType = parameter.DbType
            };
            if (parameter.BigQueryDbType.HasValue)
            {
                copy.BigQueryDbType = parameter.BigQueryDbType.Value;
            }
            parameter = copy;
        }

        _parameters.Insert(index, parameter);
        parameter.Collection = this;
    }

    public override void Remove(object value)
    {
        ArgumentNullException.ThrowIfNull(value);

        if (value is not BigQueryParameter parameter) return;
        _parameters.Remove(parameter);
    }

    public override void RemoveAt(int index) => _parameters.RemoveAt(index);

    public override void RemoveAt(string parameterName)
    {
        var index = IndexOf(parameterName);
        if (index >= 0)
        {
            RemoveAt(index);
        }
    }

    protected override DbParameter GetParameter(int index)
    {
        if (index < 0 || index >= _parameters.Count)
        {
            throw new IndexOutOfRangeException();
        }
        return _parameters[index];
    }

    protected override DbParameter GetParameter(string parameterName)
    {
        var index = IndexOf(parameterName);
        if (index < 0)
        {
            throw new IndexOutOfRangeException($"Parameter '{parameterName}' not found in the collection.");
        }
        return GetParameter(index);
    }

    protected override void SetParameter(int index, DbParameter value)
    {
        ArgumentNullException.ThrowIfNull(value);
        if (value is not BigQueryParameter parameter)
        {
            throw new ArgumentException($"Value must be of type {nameof(BigQueryParameter)}.", nameof(value));
        }
        if (index < 0 || index >= _parameters.Count)
        {
            throw new IndexOutOfRangeException();
        }
        var existingIndex = IndexOf(parameter.ParameterName);
        if (existingIndex >= 0 && existingIndex != index)
        {
            throw new ArgumentException($"Parameter '{parameter.ParameterName}' already exists in the collection at a different index.");
        }
        _parameters[index] = parameter;
    }

    protected override void SetParameter(string parameterName, DbParameter value)
    {
        ArgumentNullException.ThrowIfNull(value);
        if (value is not BigQueryParameter parameter)
        {
            throw new ArgumentException($"Value must be of type {nameof(BigQueryParameter)}.", nameof(value));
        }

        var index = IndexOf(parameterName);
        if (index < 0)
        {
            throw new IndexOutOfRangeException($"Parameter '{parameterName}' not found in the collection.");
        }

        var normalizedNewName = parameter.ParameterName.StartsWith("@") ? parameter.ParameterName : "@" + parameter.ParameterName;
        var normalizedOldName = parameterName.StartsWith("@") ? parameterName : "@" + parameterName;
        if (!string.Equals(normalizedNewName, normalizedOldName, StringComparison.OrdinalIgnoreCase))
        {
            throw new ArgumentException($"Cannot change parameter name when setting by name. Attempted to replace '{parameterName}' with '{parameter.ParameterName}'.");
        }
        SetParameter(index, parameter);
    }

    public override IEnumerator GetEnumerator() => _parameters.GetEnumerator();

    public override void CopyTo(Array array, int index) => ((IList)_parameters).CopyTo(array, index);
    public void CopyTo(BigQueryParameter[] array, int index) => _parameters.CopyTo(array, index);

    internal IList<Google.Cloud.BigQuery.V2.BigQueryParameter> ToBigQueryParameters(string? commandText = null)
    {
        if (Count == 0)
        {
            return null;
        }

        var bqParams = new List<Google.Cloud.BigQuery.V2.BigQueryParameter>(Count);
        var addedParamNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var param in _parameters)
        {
            // If command text is provided, only include parameters that are actually referenced
            // This allows us to use SQL literals for STRUCT/ARRAY<STRUCT> while still having EFCore create parameters (which we then skip)
            if (commandText != null && !string.IsNullOrEmpty(param.ParameterName))
            {
                var paramName = param.ParameterName.StartsWith("@")
                    ? param.ParameterName
                    : "@" + param.ParameterName;

                if (!commandText.Contains(paramName, StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }
            }

            var normalizedName = param.ParameterName.TrimStart('@');
            if (!addedParamNames.Add(normalizedName))
            {
                continue;
            }

            bqParams.Add(param.ToBigQueryParameter());
        }

        return bqParams.Count > 0 ? bqParams : null;
    }
}