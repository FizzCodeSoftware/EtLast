namespace FizzCode.EtLast;

using System;
using System.Collections.Generic;

public sealed class AdditionalData
{
    private readonly Dictionary<string, object> _data = new(StringComparer.OrdinalIgnoreCase);

    public object this[string key]
    {
        get => GetAs<object>(key, null);
        set => _data[key] = value;
    }

    public T GetAs<T>(string key, T defaultValue)
    {
        _data.TryGetValue(key, out var value);
        if (value != null && value is T t)
        {
            return t;
        }

        return defaultValue;
    }

    public IEnumerable<KeyValuePair<string, object>> All()
    {
        return _data;
    }
}
