namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;
    using System.Linq;

    [DebuggerDisplay("{" + nameof(ToDebugString) + "()}")]
    public class DictionaryRow : AbstractBaseRow
    {
        public override IEnumerable<KeyValuePair<string, object>> Values => _values;

        private Dictionary<string, object> _values;

        public override bool HasValue(string column)
        {
            return _values.ContainsKey(column);
        }

        public override int ColumnCount => _values.Count;

        public override void Init(IEtlContext context, IProcess creatorProcess, int uid, IEnumerable<KeyValuePair<string, object>> initialValues)
        {
            base.Init(context, creatorProcess, uid, initialValues);

            _values = initialValues == null
                ? new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase)
                : new Dictionary<string, object>(initialValues.Where(kvp => kvp.Value != null), StringComparer.OrdinalIgnoreCase);
        }

        protected override object GetValueImpl(string column)
        {
            return _values.TryGetValue(column, out var value)
                ? value
                : null;
        }

        public override string GenerateKey(params string[] columns)
        {
            if (columns.Length == 1)
            {
                return _values.TryGetValue(columns[0], out var value)
                    ? DefaultValueFormatter.Format(value)
                    : null;
            }

            return string.Join("\0", columns.Select(c => FormatToString(c, CultureInfo.InvariantCulture) ?? "-"));
        }

        public override string GenerateKeyUpper(params string[] columns)
        {
            if (columns.Length == 1)
            {
                return _values.TryGetValue(columns[0], out var value)
                    ? DefaultValueFormatter.Format(value).ToUpperInvariant()
                    : null;
            }

            return string.Join("\0", columns.Select(c => FormatToString(c, CultureInfo.InvariantCulture) ?? "-")).ToUpperInvariant();
        }

        public override void SetValue(string column, object newValue)
        {
            if (HasStaging)
                throw new ProcessExecutionException(CurrentProcess, this, "can't call " + nameof(SetValue) + " on a row with uncommitted staging");

            var hasPreviousValue = _values.TryGetValue(column, out var previousValue);
            if (newValue == null && hasPreviousValue)
            {
                foreach (var listener in Context.Listeners)
                {
                    listener.OnRowValueChanged(CurrentProcess, this, new[] { new KeyValuePair<string, object>(column, newValue) });
                }

                _values.Remove(column);
            }
            else if (!hasPreviousValue || newValue != previousValue)
            {
                foreach (var listener in Context.Listeners)
                {
                    listener.OnRowValueChanged(CurrentProcess, this, new KeyValuePair<string, object>(column, newValue));
                }

                _values[column] = newValue;
            }
        }

        public override void SetStagedValue(string column, object newValue)
        {
            var hasPreviousValue = _values.TryGetValue(column, out var previousValue);
            if ((!hasPreviousValue && newValue == null)
                || (hasPreviousValue && newValue == previousValue))
            {
                if (Staging?.ContainsKey(column) == true)
                    Staging.Remove(column);

                return;
            }

            if (Staging == null)
                Staging = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);

            Staging[column] = newValue;
        }

        public override void ApplyStaging()
        {
            if (!HasStaging)
                return;

            foreach (var kvp in Staging)
            {
                if (kvp.Value == null)
                {
                    _values.Remove(kvp.Key);
                }
                else
                {
                    _values[kvp.Key] = kvp.Value;
                }
            }

            foreach (var listener in Context.Listeners)
            {
                listener.OnRowValueChanged(CurrentProcess, this, Staging.ToArray());
            }

            Staging.Clear();
        }
    }
}