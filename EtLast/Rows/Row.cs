using System.Drawing;

namespace FizzCode.EtLast;

[DebuggerDisplay("{" + nameof(ToDebugString) + "()}")]
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed class Row(IEtlContext context, IProcess process, long id, IEnumerable<KeyValuePair<string, object>> initialValues) : IRow
{
    public IProcess Owner { get; private set; } = process;
    public long Id { get; } = id;

    public IEnumerable<KeyValuePair<string, object>> Values => _values;
    public int ValueCount => _values.Count;

    public IEnumerable<KeyValuePair<string, object>> NotNullValues => _values.Where(x => x.Value != null);
    public object Tag { get; set; }

    internal Dictionary<string, object> _values = initialValues == null
            ? new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase)
            : new Dictionary<string, object>(initialValues, StringComparer.OrdinalIgnoreCase);

    private readonly IEtlContext _context = context;

    public void SetOwner(IProcess newOwner)
    {
        if (Owner == newOwner)
            return;

        var previousOwner = Owner;
        Owner = newOwner;

        foreach (var listener in _context.RowListeners)
            listener.OnRowOwnerChanged(this, previousOwner, newOwner);
    }

    public object this[string column]
    {
        get => _values.TryGetValue(column, out var value) ? value : null;
        set
        {
            if (_context.RowListeners.Count > 0)
            {
                var stored = _values.TryGetValue(column, out var previousValue);
                if (value == null)
                {
                    if (previousValue != null)
                    {
                        foreach (var listener in _context.RowListeners)
                            listener.OnRowValueChanged(this, [new KeyValuePair<string, object>(column, null)]);
                    }

                    _values[column] = null;
                }
                else if (value is EtlRowRemovedValue)
                {
                    if (stored)
                    {
                        _values.Remove(column);
                        foreach (var listener in _context.RowListeners)
                            listener.OnRowValueChanged(this, [new KeyValuePair<string, object>(column, value)]);
                    }
                }
                else if (previousValue == null || value != previousValue)
                {
                    foreach (var listener in _context.RowListeners)
                    {
                        listener.OnRowValueChanged(this, new KeyValuePair<string, object>(column, value));
                    }

                    _values[column] = value;
                }
            }
            else
            {
                if (value is EtlRowRemovedValue)
                {
                    _values.Remove(column);
                }
                else
                {
                    _values[column] = value;
                }
            }
        }
    }

    public string ToDebugString(bool multiLine = false)
    {
        if (!multiLine)
        {
            return Id != -1
                ? "ID: "
                    + Id.ToString("D", CultureInfo.InvariantCulture)
                    + (Tag != null ? ", tag: " + Tag.ToString() : "")
                    + (_values.Count > 0
                        ? ", " + string.Join(", ", _values.Select(kvp => "[" + kvp.Key + "] = " + (kvp.Value != null ? kvp.Value.ToString() + " (" + kvp.Value.GetType().GetFriendlyTypeName() + ")" : "NULL")))
                        : "no values")
                : (Tag != null ? "tag: " + Tag.ToString() + ", " : "")
                    + (_values.Count > 0
                        ? string.Join(", ", _values.Select(kvp => "[" + kvp.Key + "] = " + (kvp.Value != null ? kvp.Value.ToString() + " (" + kvp.Value.GetType().GetFriendlyTypeName() + ")" : "NULL")))
                        : "no values");
        }
        else
        {
            return Id != -1
                ? "ID: "
                    + Id.ToString("D", CultureInfo.InvariantCulture)
                    + (Tag != null ? "\ttag: " + Tag.ToString() : "")
                    + (_values.Count > 0
                        ? "\n" + string.Join("\n", _values.Select(kvp => "[" + kvp.Key + "] = " + (kvp.Value != null ? kvp.Value.ToString() + " (" + kvp.Value.GetType().GetFriendlyTypeName() + ")" : "NULL")))
                        : "no values")
                : (Tag != null ? "tag: " + Tag.ToString() + "\n" : "")
                    + (_values.Count > 0
                        ? string.Join("\n", _values.Select(kvp => "[" + kvp.Key + "] = " + (kvp.Value != null ? kvp.Value.ToString() + " (" + kvp.Value.GetType().GetFriendlyTypeName() + ")" : "NULL")))
                        : "no values");
        }
    }

    /// <summary>
    /// Returns true if any value is <see cref="EtlRowError"/>.
    /// </summary>
    /// <returns>True if any value is <see cref="EtlRowError"/>.</returns>
    public bool HasError()
    {
        return _values.Any(x => x.Value is EtlRowError);
    }

    public T GetAs<T>(string column)
    {
        var value = _values.TryGetValue(column, out var v) ? v : null;
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
        if (!_values.TryGetValue(column, out var value) || value is null)
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
        _values.TryGetValue(column, out var currentValue);
        return DefaultValueComparer.ValuesAreEqual(currentValue, value);
    }

    public bool IsNullOrEmpty(string column)
    {
        _values.TryGetValue(column, out var value);
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
        _values.TryGetValue(column, out var value);
        return value is T;
    }

    public string FormatToString(string column, IValueFormatter formatter = null, IFormatProvider formatProvider = null)
    {
        _values.TryGetValue(column, out var value);
        return (formatter ?? ValueFormatter.Default).Format(value, formatProvider);
    }

    public bool HasValue(string column)
    {
        return _values.TryGetValue(column, out var value) && value != null;
    }

    public string GenerateKey(params string[] columns)
    {
        if (columns.Length == 1)
            return FormatToString(columns[0], ValueFormatter.Default, CultureInfo.InvariantCulture);

        return string.Join("\0", columns.Select(c => FormatToString(c, ValueFormatter.Default, CultureInfo.InvariantCulture) ?? "-"));
    }

    public string GenerateKeyUpper(params string[] columns)
    {
        if (columns.Length == 1)
            return FormatToString(columns[0], ValueFormatter.Default, CultureInfo.InvariantCulture)?.ToUpperInvariant();

        return string.Join("\0", columns.Select(c => FormatToString(c, ValueFormatter.Default, CultureInfo.InvariantCulture) ?? "-"))?.ToUpperInvariant();
    }

    public void MergeWith(IEnumerable<KeyValuePair<string, object>> values)
    {
        if (_context.RowListeners.Count > 0)
        {
            List<KeyValuePair<string, object>> changedValues = null;
            foreach (var kvp in values)
            {
                var stored = _values.TryGetValue(kvp.Key, out var currentValue);

                if (kvp.Value == null)
                {
                    if (currentValue != null)
                    {
                        changedValues ??= [];
                        changedValues.Add(kvp);
                        _values[kvp.Key] = null;
                    }
                    else
                    {
                        if (!stored)
                            _values[kvp.Key] = null;
                    }
                }
                else if (kvp.Value is EtlRowRemovedValue)
                {
                    if (stored)
                    {
                        _values.Remove(kvp.Key);
                        changedValues ??= [];
                        changedValues.Add(kvp);
                    }
                }
                else if (currentValue == null || kvp.Value != currentValue)
                {
                    changedValues ??= [];
                    changedValues.Add(kvp);

                    if (kvp.Value != null)
                        _values[kvp.Key] = kvp.Value;
                }
            }

            if (changedValues != null)
            {
                foreach (var listener in _context.RowListeners)
                {
                    listener.OnRowValueChanged(this, [.. changedValues]);
                }
            }
        }
        else
        {
            foreach (var kvp in values)
            {
                if (kvp.Value is EtlRowRemovedValue)
                {
                    _values.Remove(kvp.Key);
                }
                else
                {
                    _values[kvp.Key] = kvp.Value;
                }
            }
        }
    }

    public void Clear()
    {
        if (_values.Count == 0)
            return;

        if (_context.RowListeners.Count > 0)
        {
            List<KeyValuePair<string, object>> changedValues = null;
            foreach (var kvp in _values)
            {
                if (kvp.Value != null)
                {
                    changedValues ??= [];
                    changedValues.Add(new KeyValuePair<string, object>(kvp.Key, null));
                }
            }

            _values.Clear();

            if (changedValues != null)
            {
                foreach (var listener in _context.RowListeners)
                {
                    listener.OnRowValueChanged(this, [.. changedValues]);
                }
            }
        }
        else
        {
            _values.Clear();
        }
    }

    public void RemoveColumns(params string[] columns)
    {
        if (_context.RowListeners.Count > 0)
        {
            List<KeyValuePair<string, object>> changedValues = null;
            foreach (var col in columns)
            {
                _values.Remove(col, out var value);
                if (value != null)
                {
                    changedValues ??= [];
                    changedValues.Add(new KeyValuePair<string, object>(col, null));
                }
            }

            if (changedValues != null)
            {
                foreach (var listener in _context.RowListeners)
                {
                    listener.OnRowValueChanged(this, [.. changedValues]);
                }
            }
        }
        else
        {
            foreach (var col in columns)
                _values.Remove(col);
        }
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

        foreach (var kvp in _values)
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