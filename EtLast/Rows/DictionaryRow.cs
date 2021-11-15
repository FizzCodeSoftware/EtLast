namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;
    using System.Linq;

    [DebuggerDisplay("{" + nameof(ToDebugString) + "()}")]
    public sealed class DictionaryRow : IRow
    {
        public IEtlContext Context { get; private set; }
        public IProcess CreatorProcess { get; private set; }
        public IProcess CurrentProcess { get; set; }
        public int Uid { get; private set; }

        public int ColumnCount => _values.Count;

        public IEnumerable<KeyValuePair<string, object>> Values => _values;

        internal Dictionary<string, object> _values;

        public object Tag { get; set; }

        public object this[string column]
        {
            get => _values.TryGetValue(column, out var value) ? value : null;
            set
            {
                _values.TryGetValue(column, out var previousValue);
                if (value == null)
                {
                    if (previousValue != null)
                    {
                        foreach (var listener in Context.Listeners)
                        {
                            listener.OnRowValueChanged(this, new[] { new KeyValuePair<string, object>(column, null) });
                        }

                        _values.Remove(column);
                    }
                }
                else if (previousValue == null || value != previousValue)
                {
                    foreach (var listener in Context.Listeners)
                    {
                        listener.OnRowValueChanged(this, new KeyValuePair<string, object>(column, value));
                    }

                    _values[column] = value;
                }
            }
        }

        public string ToDebugString(bool multiLine = false)
        {
            if (!multiLine)
            {
                return "UID: "
                    + Uid.ToString("D", CultureInfo.InvariantCulture)
                    + (Tag != null ? ", tag: " + Tag.ToString() : "")
                    + (_values.Count > 0
                        ? ", " + string.Join(", ", _values.Select(kvp => "[" + kvp.Key + "] = " + (kvp.Value != null ? kvp.Value.ToString() + " (" + kvp.Value.GetType().GetFriendlyTypeName() + ")" : "NULL")))
                        : "no values");
            }
            else
            {
                return "UID: "
                    + Uid.ToString("D", CultureInfo.InvariantCulture)
                    + (Tag != null ? "\ttag: " + Tag.ToString() : "")
                    + (_values.Count > 0
                        ? "\n" + string.Join("\n", _values.Select(kvp => "[" + kvp.Key + "] = " + (kvp.Value != null ? kvp.Value.ToString() + " (" + kvp.Value.GetType().GetFriendlyTypeName() + ")" : "NULL")))
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
                exception.Data.Add("Column", column);
                exception.Data.Add("Value", value != null ? value.ToString() : "NULL");
                exception.Data.Add("SourceType", (value?.GetType()).GetFriendlyTypeName());
                exception.Data.Add("TargetType", TypeHelpers.GetFriendlyTypeName(typeof(T)));
                throw exception;
            }
        }

        public T GetAs<T>(string column, T defaultValueIfNull)
        {
            if (!_values.TryGetValue(column, out var value))
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
                else
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

        public string FormatToString(string column, IFormatProvider formatProvider = null)
        {
            _values.TryGetValue(column, out var value);
            return DefaultValueFormatter.Format(value);
        }

        public void Init(IEtlContext context, IProcess creatorProcess, int uid, IEnumerable<KeyValuePair<string, object>> initialValues)
        {
            Context = context;
            CreatorProcess = creatorProcess;
            CurrentProcess = creatorProcess;
            Uid = uid;

            _values = initialValues == null
                ? new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase)
                : new Dictionary<string, object>(initialValues.Where(kvp => kvp.Value != null), StringComparer.OrdinalIgnoreCase);
        }

        public bool HasValue(string column)
        {
            return _values.ContainsKey(column);
        }

        public string GenerateKey(params string[] columns)
        {
            if (columns.Length == 1)
            {
                return _values.TryGetValue(columns[0], out var value)
                    ? DefaultValueFormatter.Format(value)
                    : null;
            }

            return string.Join("\0", columns.Select(c => FormatToString(c, CultureInfo.InvariantCulture) ?? "-"));
        }

        public string GenerateKeyUpper(params string[] columns)
        {
            if (columns.Length == 1)
            {
                return _values.TryGetValue(columns[0], out var value)
                    ? DefaultValueFormatter.Format(value).ToUpperInvariant()
                    : null;
            }

            return string.Join("\0", columns.Select(c => FormatToString(c, CultureInfo.InvariantCulture) ?? "-")).ToUpperInvariant();
        }

        public void MergeWith(IEnumerable<KeyValuePair<string, object>> values)
        {
            List<KeyValuePair<string, object>> changedValues = null;
            foreach (var kvp in values)
            {
                _values.TryGetValue(kvp.Key, out var currentValue);

                if (kvp.Value == null)
                {
                    if (currentValue != null)
                    {
                        if (changedValues == null)
                            changedValues = new List<KeyValuePair<string, object>>();

                        changedValues.Add(kvp);
                        _values.Remove(kvp.Key);
                    }
                }
                else if (currentValue == null || kvp.Value != currentValue)
                {
                    if (changedValues == null)
                        changedValues = new List<KeyValuePair<string, object>>();

                    changedValues.Add(kvp);

                    if (kvp.Value != null)
                        _values[kvp.Key] = kvp.Value;
                }
            }

            if (changedValues != null)
            {
                foreach (var listener in Context.Listeners)
                {
                    listener.OnRowValueChanged(this, changedValues.ToArray());
                }
            }
        }
    }
}