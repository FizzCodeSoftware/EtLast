namespace FizzCode.EtLast;

[DebuggerDisplay("{" + nameof(ToDebugString) + "()}")]
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed class TrackedRow(IRow originalRow) : IRow
{
    public IProcess Owner => originalRow.Owner;
    public long Id => originalRow.Id;
    public object Tag { get => originalRow.Tag; set => originalRow.Tag = value; }

    public IEnumerable<KeyValuePair<string, object>> Values
    {
        get
        {
            return _changes?.Count > 0
                ? GetCurrentValues()
                : originalRow.Values;
        }
    }

    public int ValueCount => Values.Count();

    public IEnumerable<KeyValuePair<string, object>> NotNullValues
    {
        get
        {
            return _changes?.Count > 0
                ? GetCurrentValues().Where(x => x.Value != null)
                : originalRow.NotNullValues;
        }
    }

    private IEnumerable<KeyValuePair<string, object>> GetCurrentValues()
    {
        foreach (var kvp in originalRow.Values)
        {
            if (!_changes.ContainsKey(kvp.Key))
                yield return kvp;
        }

        foreach (var kvp in _changes)
            yield return kvp;
    }

    private Dictionary<string, object> _changes;

    public object this[string column]
    {
        get
        {
            if (_changes?.Count > 0 && _changes.TryGetValue(column, out var stagedValue))
                return stagedValue;

            return originalRow[column];
        }
        set
        {
            var originalValue = originalRow[column];

            if (originalValue != null && value == originalValue)
            {
                if (_changes?.ContainsKey(column) == true)
                    _changes.Remove(column);

                return;
            }

            _changes ??= new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
            _changes[column] = value;
        }
    }

    public void ApplyChanges()
    {
        if (_changes == null)
            return;

        if (_changes.Count > 0)
        {
            originalRow.MergeWith(_changes);
            _changes.Clear();
        }

        _changes = null;
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
            exception.Data["Column"] = column;
            exception.Data["Value"] = value != null ? value.ToString() : "NULL";
            if (value != null)
                exception.Data["ValueType"] = value.GetType().GetFriendlyTypeName();
            exception.Data["RequestedType"] = TypeHelpers.GetFriendlyTypeName(typeof(T));
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
            var exception = new InvalidCastException("error raised during a cast operation", ex);
            exception.Data["Column"] = column;
            exception.Data["Value"] = value.ToString();
            exception.Data["ValueType"] = value.GetType().GetFriendlyTypeName();
            exception.Data["RequestedType"] = TypeHelpers.GetFriendlyTypeName(typeof(T));
            throw exception;
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
            else if (kvp.Value != null)
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
            return "ID: "
                + Id.ToString("D", CultureInfo.InvariantCulture)
                + (Tag != null ? ", tag: " + Tag.ToString() : "")
                + (Values.Any()
                    ? ", " + string.Join(", ", Values.Select(kvp => "[" + kvp.Key + "] = " + (kvp.Value != null ? kvp.Value.ToString() + " (" + kvp.Value.GetType().GetFriendlyTypeName() + ")" : "NULL")))
                    : "no values");
        }
        else
        {
            return "ID: "
                + Id.ToString("D", CultureInfo.InvariantCulture)
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

    public void RemoveColumns(params string[] columns)
    {
        foreach (var col in columns)
        {
            _changes.Remove(col);
        }
    }

    public void SetOwner(IProcess currentProcess)
    {
        originalRow.SetOwner(currentProcess);
    }
}