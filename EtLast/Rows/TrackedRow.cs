using System.Drawing;

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
            exception.Data["RequestedType"] = TypeExtensions.GetFriendlyTypeName(typeof(T));
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
            exception.Data["RequestedType"] = TypeExtensions.GetFriendlyTypeName(typeof(T));
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
            return (Tag != null ? ", tag: " + Tag.ToString() + ", " : "")
                + (Values.Any()
                    ? string.Join(", ", Values.Select(kvp => "[" + kvp.Key + "] = " + (kvp.Value != null ? kvp.Value.ToString() + " (" + kvp.Value.GetType().GetFriendlyTypeName() + ")" : "NULL")))
                    : "no values");
        }
        else
        {
            return (Tag != null ? "tag: " + Tag.ToString() + "\n" : "")
                + (Values.Any()
                    ? string.Join("\n", Values.Select(kvp => "[" + kvp.Key + "] = " + (kvp.Value != null ? kvp.Value.ToString() + " (" + kvp.Value.GetType().GetFriendlyTypeName() + ")" : "NULL")))
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

    public long GetRowChecksumForSpecificColumns(string[] columns)
    {
        Span<byte> buffer = stackalloc byte[16];

        var hash = 0xCBF29CE484222325;

        foreach (var col in columns)
        {
            var value = this[col];
            if (value != null)
            {
                if (value is int iv)
                {
                    hash ^= unchecked((uint)iv);
                    hash *= 0x100000001B3;
                }
                else if (value is uint uiv)
                {
                    hash ^= uiv;
                    hash *= 0x100000001B3;
                }
                else if (value is long lv)
                {
                    hash ^= unchecked((ulong)lv);
                    hash *= 0x100000001B3;
                }
                else if (value is ulong ulv)
                {
                    hash ^= ulv;
                    hash *= 0x100000001B3;
                }
                else if (value is sbyte sbv)
                {
                    hash ^= (byte)sbv;
                    hash *= 0x100000001B3;
                }
                else if (value is byte bv)
                {
                    hash ^= bv;
                    hash *= 0x100000001B3;
                }
                else if (value is short sv)
                {
                    hash ^= unchecked((ushort)sv);
                    hash *= 0x100000001B3;
                }
                else if (value is ushort usv)
                {
                    hash ^= usv;
                    hash *= 0x100000001B3;
                }
                else if (value is string str)
                {
                    for (var i = 0; i < str.Length; i++)
                    {
                        hash ^= (byte)str[i];
                        hash *= 0x100000001B3;
                    }
                    continue;
                }
                else if (value is DateTime dt)
                {
                    hash ^= unchecked((ulong)dt.Ticks);
                    hash *= 0x100000001B3;
                }
                else if (value is DateTimeOffset dto)
                {
                    hash ^= unchecked((ulong)dto.Ticks);
                    hash *= 0x100000001B3;
                    hash ^= unchecked((ulong)dto.Offset.Ticks);
                    hash *= 0x100000001B3;
                }
                else if (value is TimeSpan ts)
                {
                    hash ^= unchecked((ulong)ts.Ticks);
                    hash *= 0x100000001B3;
                }
                else if (value is Guid guidv)
                {
                    guidv.TryWriteBytes(buffer);
                    for (var i = 0; i < 16; i++)
                    {
                        hash ^= buffer[i];
                        hash *= 0x100000001B3;
                    }
                }
                else if (value is bool boolcv)
                {
                    hash ^= boolcv ? 39u : 999u;
                    hash *= 0x100000001B3;
                }
                else if (value is float fv)
                {
                    MemoryMarshal.Write(buffer, in fv);
                    for (var i = 0; i < 4; i++)
                    {
                        hash ^= buffer[i];
                        hash *= 0x100000001B3;
                    }
                }
                else if (value is double dv)
                {
                    MemoryMarshal.Write(buffer, in dv);
                    for (var i = 0; i < 8; i++)
                    {
                        hash ^= buffer[i];
                        hash *= 0x100000001B3;
                    }
                }
                else if (value is decimal dec)
                {
                    MemoryMarshal.Write(buffer, in dec);
                    for (var i = 0; i < 16; i++)
                    {
                        hash ^= buffer[i];
                        hash *= 0x100000001B3;
                    }
                }
                else if (value is Half half)
                {
                    MemoryMarshal.Write(buffer, in half);
                    hash ^= buffer[0];
                    hash *= 0x100000001B3;
                    hash ^= buffer[1];
                    hash *= 0x100000001B3;
                }
                else if (value is byte[] ba)
                {
                    for (var i = 0; i < ba.Length; i++)
                    {
                        hash ^= ba[i];
                        hash *= 0x100000001B3;
                    }
                }
                else if (value is char cv)
                {
                    hash ^= (byte)cv;
                    hash *= 0x100000001B3;
                }
                else if (value is UInt128 ui128)
                {
                    hash ^= (ulong)(ui128 >> 64);
                    hash ^= (ulong)ui128;
                }
                else if (value is Int128 i128)
                {
                    hash ^= (ulong)(i128 >> 64);
                    hash ^= (ulong)i128;
                }
                else if (value is DateOnly don)
                {
                    hash ^= unchecked((uint)don.DayNumber);
                    hash *= 0x100000001B3;
                }
                else if (value is TimeOnly ton)
                {
                    hash ^= unchecked((ulong)ton.Ticks);
                    hash *= 0x100000001B3;
                }
                else if (value is Color color)
                {
                    hash ^= unchecked((uint)color.ToArgb());
                    hash *= 0x100000001B3;
                }
                else
                {
                    Debugger.Break();
                }
            }
        }

        var keyHashCode = unchecked((long)hash);
        return keyHashCode;
    }

    public long GetRowChecksumForAllColumns(string[] exceptColumns)
    {
        Span<byte> buffer = stackalloc byte[16];

        var hash = 0xCBF29CE484222325;

        foreach (var kvp in Values)
        {
            if (exceptColumns?.Length > 0)
            {
                var ok = false;
                for (var i = 0; i < exceptColumns.Length; i++)
                {
                    if (string.Equals(exceptColumns[i], kvp.Key, StringComparison.InvariantCultureIgnoreCase))
                    {
                        ok = false;
                        break;
                    }
                }
                if (!ok)
                    continue;
            }

            var value = kvp.Value;
            if (value != null)
            {
                if (value is int iv)
                {
                    hash ^= unchecked((uint)iv);
                    hash *= 0x100000001B3;
                }
                else if (value is uint uiv)
                {
                    hash ^= uiv;
                    hash *= 0x100000001B3;
                }
                else if (value is long lv)
                {
                    hash ^= unchecked((ulong)lv);
                    hash *= 0x100000001B3;
                }
                else if (value is ulong ulv)
                {
                    hash ^= ulv;
                    hash *= 0x100000001B3;
                }
                else if (value is sbyte sbv)
                {
                    hash ^= (byte)sbv;
                    hash *= 0x100000001B3;
                }
                else if (value is byte bv)
                {
                    hash ^= bv;
                    hash *= 0x100000001B3;
                }
                else if (value is short sv)
                {
                    hash ^= unchecked((ushort)sv);
                    hash *= 0x100000001B3;
                }
                else if (value is ushort usv)
                {
                    hash ^= usv;
                    hash *= 0x100000001B3;
                }
                else if (value is string str)
                {
                    for (var i = 0; i < str.Length; i++)
                    {
                        hash ^= (byte)str[i];
                        hash *= 0x100000001B3;
                    }
                    continue;
                }
                else if (value is DateTime dt)
                {
                    hash ^= unchecked((ulong)dt.Ticks);
                    hash *= 0x100000001B3;
                }
                else if (value is DateTimeOffset dto)
                {
                    hash ^= unchecked((ulong)dto.Ticks);
                    hash *= 0x100000001B3;
                    hash ^= unchecked((ulong)dto.Offset.Ticks);
                    hash *= 0x100000001B3;
                }
                else if (value is TimeSpan ts)
                {
                    hash ^= unchecked((ulong)ts.Ticks);
                    hash *= 0x100000001B3;
                }
                else if (value is Guid guidv)
                {
                    guidv.TryWriteBytes(buffer);
                    for (var i = 0; i < 16; i++)
                    {
                        hash ^= buffer[i];
                        hash *= 0x100000001B3;
                    }
                }
                else if (value is bool boolcv)
                {
                    hash ^= boolcv ? 39u : 999u;
                    hash *= 0x100000001B3;
                }
                else if (value is float fv)
                {
                    MemoryMarshal.Write(buffer, in fv);
                    for (var i = 0; i < 4; i++)
                    {
                        hash ^= buffer[i];
                        hash *= 0x100000001B3;
                    }
                }
                else if (value is double dv)
                {
                    MemoryMarshal.Write(buffer, in dv);
                    for (var i = 0; i < 8; i++)
                    {
                        hash ^= buffer[i];
                        hash *= 0x100000001B3;
                    }
                }
                else if (value is decimal dec)
                {
                    MemoryMarshal.Write(buffer, in dec);
                    for (var i = 0; i < 16; i++)
                    {
                        hash ^= buffer[i];
                        hash *= 0x100000001B3;
                    }
                }
                else if (value is Half half)
                {
                    MemoryMarshal.Write(buffer, in half);
                    hash ^= buffer[0];
                    hash *= 0x100000001B3;
                    hash ^= buffer[1];
                    hash *= 0x100000001B3;
                }
                else if (value is byte[] ba)
                {
                    for (var i = 0; i < ba.Length; i++)
                    {
                        hash ^= ba[i];
                        hash *= 0x100000001B3;
                    }
                }
                else if (value is char cv)
                {
                    hash ^= (byte)cv;
                    hash *= 0x100000001B3;
                }
                else if (value is UInt128 ui128)
                {
                    hash ^= (ulong)(ui128 >> 64);
                    hash ^= (ulong)ui128;
                }
                else if (value is Int128 i128)
                {
                    hash ^= (ulong)(i128 >> 64);
                    hash ^= (ulong)i128;
                }
                else if (value is DateOnly don)
                {
                    hash ^= unchecked((uint)don.DayNumber);
                    hash *= 0x100000001B3;
                }
                else if (value is TimeOnly ton)
                {
                    hash ^= unchecked((ulong)ton.Ticks);
                    hash *= 0x100000001B3;
                }
                else if (value is Color color)
                {
                    hash ^= unchecked((uint)color.ToArgb());
                    hash *= 0x100000001B3;
                }
                else
                {
                    Debugger.Break();
                }
            }
        }

        var keyHashCode = unchecked((long)hash);
        return keyHashCode;
    }
}