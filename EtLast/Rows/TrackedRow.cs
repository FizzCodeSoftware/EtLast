namespace FizzCode.EtLast;

public sealed class TrackedRow : IRow
{
    private readonly IRow _originalRow;

    public IProcess CurrentProcess { get => _originalRow.CurrentProcess; set => _originalRow.CurrentProcess = value; }
    public IEtlContext Context => _originalRow.Context;
    public int Uid => _originalRow.Uid;
    public IProcess CreatorProcess => _originalRow.CreatorProcess;
    public object Tag { get => _originalRow.Tag; set => _originalRow.Tag = value; }
    public IEnumerable<KeyValuePair<string, object>> Values
    {
        get
        {
            if (_staging?.Count > 0)
            {
                return GetCurrentValues();
            }
            else
            {
                return _originalRow.Values;
            }
        }
    }

    private IEnumerable<KeyValuePair<string, object>> GetCurrentValues()
    {
        foreach (var kvp in _originalRow.Values)
        {
            if (!_staging.ContainsKey(kvp.Key))
                yield return kvp;
        }

        foreach (var kvp in _staging)
        {
            if (kvp.Value != null)
                yield return kvp;
        }
    }

    public int ColumnCount => Values.Count();

    private Dictionary<string, object> _staging;

    public object this[string column]
    {
        get
        {
            if (_staging?.Count > 0 && _staging.TryGetValue(column, out var stagedValue))
                return stagedValue;

            return _originalRow[column];
        }
        set
        {
            var originalValue = _originalRow[column];
            if ((originalValue == null && value == null)
                || (originalValue != null && value == originalValue))
            {
                if (_staging?.ContainsKey(column) == true)
                    _staging.Remove(column);

                return;
            }

            if (_staging == null)
                _staging = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);

            _staging[column] = value;
        }
    }

    public TrackedRow(IRow originalRow)
    {
        _originalRow = originalRow;
    }

    public void ApplyChanges()
    {
        if (_staging == null)
            return;

        if (_staging.Count > 0)
        {
            _originalRow.MergeWith(_staging);
            _staging.Clear();
        }

        _staging = null;
    }

    public void Init(IEtlContext context, IProcess creatorProcess, int uid, IEnumerable<KeyValuePair<string, object>> initialValues)
    {
    }

    public void MergeWith(IEnumerable<KeyValuePair<string, object>> values)
    {
        foreach (var kvp in values)
        {
            this[kvp.Key] = kvp.Value;
        }
    }

    public bool HasError()
    {
        return Values.Any(x => x.Value is EtlRowError);
    }

    public T GetAs<T>(string column)
    {
        var value = this[column];
        try
        {
            return (T)value;
        }
        catch (Exception ex)
        {
            var exception = new InvalidCastException("error raised during a cast operation", ex);
            exception.Data.Add("Column", column);
            exception.Data.Add("Value", value != null ? value.ToString() : "NULL");
            exception.Data.Add("SourceType", (value?.GetType()).GetFriendlyTypeName());
            exception.Data.Add("TargetType", TypeHelpers.GetFriendlyTypeName(typeof(T)));
            throw exception;
        }
    }

    public T GetAs<T>(string column, T defaultValueIfNull)
    {
        var value = this[column];
        if (value == null)
            return defaultValueIfNull;

        try
        {
            return (T)value;
        }
        catch (Exception ex)
        {
            throw new InvalidCastException("requested cast to '" + typeof(T).GetFriendlyTypeName() + "' is not possible of '" + (value != null ? (value.ToString() + " (" + value.GetType().GetFriendlyTypeName() + ")") : "NULL") + "' in '" + column + "'", ex);
        }
    }

    public bool Equals<T>(string column, T value)
    {
        var currentValue = this[column];
        return DefaultValueComparer.ValuesAreEqual(currentValue, value);
    }

    public bool HasValue(string column)
    {
        return this[column] != null;
    }

    public bool IsNullOrEmpty(string column)
    {
        var value = this[column];
        return value == null || (value is string str && string.IsNullOrEmpty(str));
    }

    public bool IsNullOrEmpty()
    {
        foreach (var kvp in Values)
        {
            if (kvp.Value is string str)
            {
                if (!string.IsNullOrEmpty(str))
                    return false;
            }
            else
            {
                return false;
            }
        }

        return true;
    }

    public bool Is<T>(string column)
    {
        var value = this[column];
        return value is T;
    }

    public string FormatToString(string column, IValueFormatter formatter = null, IFormatProvider formatProvider = null)
    {
        var value = this[column];
        return (formatter ?? ValueFormatter.Default).Format(value);
    }

    public string GenerateKey(params string[] columns)
    {
        if (columns.Length == 1)
        {
            var value = this[columns[0]];
            return value != null
                ? ValueFormatter.Default.Format(value)
                : null;
        }

        return string.Join("\0", columns.Select(c => FormatToString(c, ValueFormatter.Default, CultureInfo.InvariantCulture) ?? "-"));
    }

    public string GenerateKeyUpper(params string[] columns)
    {
        if (columns.Length == 1)
        {
            var value = this[columns[0]];
            return value != null
                ? ValueFormatter.Default.Format(value).ToUpperInvariant()
                : null;
        }

        return string.Join("\0", columns.Select(c => FormatToString(c, ValueFormatter.Default, CultureInfo.InvariantCulture) ?? "-")).ToUpperInvariant();
    }

    public string ToDebugString(bool multiLine = false)
    {
        if (!multiLine)
        {
            return "UID: "
                + Uid.ToString("D", CultureInfo.InvariantCulture)
                + (Tag != null ? ", tag: " + Tag.ToString() : "")
                + (Values.Any()
                    ? ", " + string.Join(", ", Values.Select(kvp => "[" + kvp.Key + "] = " + (kvp.Value != null ? kvp.Value.ToString() + " (" + kvp.Value.GetType().GetFriendlyTypeName() + ")" : "NULL")))
                    : "no values");
        }
        else
        {
            return "UID: "
                + Uid.ToString("D", CultureInfo.InvariantCulture)
                + (Tag != null ? "\ntag: " + Tag.ToString() : "")
                + (Values.Any()
                    ? "\n" + string.Join("\n", Values.Select(kvp => "[" + kvp.Key + "] = " + (kvp.Value != null ? kvp.Value.ToString() + " (" + kvp.Value.GetType().GetFriendlyTypeName() + ")" : "NULL")))
                    : "no values");
        }
    }

    public void Clear()
    {
        foreach (var kvp in Values.ToList())
        {
            this[kvp.Key] = null;
        }
    }
}
