namespace FizzCode.EtLast;

[DebuggerDisplay("{" + nameof(ToDebugString) + "()}")]
public sealed class SlimRow : ISlimRow
{
    public IEnumerable<KeyValuePair<string, object>> Values => _values;
    public int ColumnCount => _values.Count;
    public object Tag { get; set; }

    private readonly Dictionary<string, object> _values;

    public SlimRow()
    {
        _values = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
    }

    public SlimRow(IEnumerable<KeyValuePair<string, object>> initialValues)
    {
        _values = new Dictionary<string, object>(initialValues, StringComparer.OrdinalIgnoreCase);
    }

    public SlimRow(IReadOnlySlimRow initialValues)
    {
        _values = initialValues is SlimRow slimRow
            ? new Dictionary<string, object>(slimRow._values, StringComparer.OrdinalIgnoreCase)
            : initialValues is Row dictionaryRow
                ? new Dictionary<string, object>(dictionaryRow._values, StringComparer.OrdinalIgnoreCase)
                : new Dictionary<string, object>(initialValues.Values, StringComparer.OrdinalIgnoreCase);
    }

    public object this[string column]
    {
        get => GetValueImpl(column);
        set
        {
            if (value != null)
                _values[column] = value;
            else
                _values.Remove(column);
        }
    }

    private object GetValueImpl(string column)
    {
        return _values.TryGetValue(column, out var value)
            ? value
            : null;
    }

    public bool HasValue(string column)
    {
        return _values.ContainsKey(column);
    }

    public string ToDebugString(bool multiLine = false)
    {
        if (!multiLine)
        {
            return
                (Tag != null ? "tag: " + Tag.ToString() : "")
                + (_values.Count > 0
                    ? string.Join(", ", _values.Select(kvp => "[" + kvp.Key + "] = " + (kvp.Value != null ? kvp.Value.ToString() + " (" + kvp.Value.GetType().GetFriendlyTypeName() + ")" : "NULL")))
                    : "no values");
        }
        else
        {
            return
                (Tag != null ? "tag: " + Tag.ToString() + "\n" : "")
                + (_values.Count > 0
                    ? string.Join("\n", _values.Select(kvp => "[" + kvp.Key + "] = " + (kvp.Value != null ? kvp.Value.ToString() + " (" + kvp.Value.GetType().GetFriendlyTypeName() + ")" : "NULL")))
                    : "no values");
        }
    }

    public void Clear()
    {
        _values.Clear();
    }

    public T GetAs<T>(string column)
    {
        var value = GetValueImpl(column);
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
        var value = GetValueImpl(column);
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
        var currentValue = GetValueImpl(column);
        return DefaultValueComparer.ValuesAreEqual(currentValue, value);
    }

    public bool IsNullOrEmpty(string column)
    {
        var value = GetValueImpl(column);
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
        return GetValueImpl(column) is T;
    }

    public string FormatToString(string column, IValueFormatter formatter = null, IFormatProvider formatProvider = null)
    {
        var value = GetValueImpl(column);
        return (formatter ?? ValueFormatter.Default).Format(value);
    }

    public bool HasError()
    {
        return Values.Any(x => x.Value is EtlRowError);
    }

    public string GenerateKey(params string[] columns)
    {
        if (columns.Length == 1)
        {
            return _values.TryGetValue(columns[0], out var value)
                ? ValueFormatter.Default.Format(value)
                : null;
        }

        return string.Join("\0", columns.Select(c => FormatToString(c, ValueFormatter.Default, CultureInfo.InvariantCulture) ?? "-"));
    }

    public string GenerateKeyUpper(params string[] columns)
    {
        if (columns.Length == 1)
        {
            return _values.TryGetValue(columns[0], out var value)
                ? ValueFormatter.Default.Format(value).ToUpperInvariant()
                : null;
        }

        return string.Join("\0", columns.Select(c => FormatToString(c, ValueFormatter.Default, CultureInfo.InvariantCulture) ?? "-")).ToUpperInvariant();
    }
}
