namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;
    using System.Linq;

    [DebuggerDisplay("{" + nameof(ToDebugString) + "()}")]
    public class SlimRow : IEditableRow, IReadOnlySlimRow
    {
        private readonly Dictionary<string, object> _values = new Dictionary<string, object>();
        public virtual IEnumerable<KeyValuePair<string, object>> Values => _values;
        public int ColumnCount => _values.Count;

        public object this[string column]
        {
            get => GetValueImpl(column);
            set => SetValue(column, value);
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

        public string ToDebugString()
        {
            return string.Join(", ", Values.Select(kvp => "[" + kvp.Key + "] = " + (kvp.Value != null ? kvp.Value.ToString() + " (" + kvp.Value.GetType().GetFriendlyTypeName() + ")" : "NULL")));
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
            if (currentValue == null && value == null)
                return false;

            return DefaultValueComparer.ValuesAreEqual(currentValue, value);
        }

        public bool IsNull(string column)
        {
            var value = GetValueImpl(column);
            return value == null;
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
                if (kvp.Value != null)
                    return false;

                if (kvp.Value is string str)
                {
                    if (!string.IsNullOrEmpty(str))
                        return false;
                }
            }

            return true;
        }

        public bool Is<T>(string column)
        {
            return GetValueImpl(column) is T;
        }

        public string FormatToString(string column, IFormatProvider formatProvider = null)
        {
            var value = GetValueImpl(column);
            return DefaultValueFormatter.Format(value);
        }

        public void SetValue(string column, object newValue)
        {
            if (newValue != null)
            {
                _values[column] = newValue;
            }
            else
            {
                _values.Remove(column);
            }
        }

        public bool HasError()
        {
            return Values.Any(x => x.Value is EtlRowError);
        }

        public string GenerateKey(params string[] columns)
        {
            if (columns.Length == 1)
            {
                return IsNull(columns[0])
                    ? null
                    : FormatToString(columns[0], CultureInfo.InvariantCulture);
            }

            return string.Join("\0", columns.Select(c => FormatToString(c, CultureInfo.InvariantCulture) ?? "-"));
        }

        public string GenerateKeyUpper(params string[] columns)
        {
            if (columns.Length == 1)
            {
                return IsNull(columns[0])
                    ? null
                    : FormatToString(columns[0], CultureInfo.InvariantCulture).ToUpperInvariant();
            }

            return string.Join("\0", columns.Select(c => FormatToString(c, CultureInfo.InvariantCulture) ?? "-")).ToUpperInvariant();
        }
    }
}