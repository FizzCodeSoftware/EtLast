﻿namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;

    public sealed class EtlSessionArguments : IEtlSessionArguments
    {
        public IEnumerable<KeyValuePair<string, object>> All => _values;

        private readonly Dictionary<string, object> _values;

        public EtlSessionArguments(Dictionary<string, object> values)
        {
            _values = values != null
                ? new Dictionary<string, object>(values, StringComparer.InvariantCultureIgnoreCase)
                : new Dictionary<string, object>(StringComparer.InvariantCultureIgnoreCase);
        }

        public T Get<T>(string key, T defaultValue = default)
        {
            if (_values.TryGetValue(key, out var value))
            {
                if (value is Func<T> f)
                    value = f.Invoke();

                if (value is T castValue)
                    return castValue;
            }

            return defaultValue;
        }
    }
}