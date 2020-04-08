namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
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
                ? new Dictionary<string, object>(StringComparer.InvariantCultureIgnoreCase)
                : new Dictionary<string, object>(initialValues.Where(kvp => kvp.Value != null), StringComparer.InvariantCultureIgnoreCase);
        }

        protected override object GetValueImpl(string column)
        {
            return _values.TryGetValue(column, out var value)
                ? value
                : null;
        }

        public override void SetValue(string column, object newValue)
        {
            if (HasStaging)
                throw new ProcessExecutionException(CurrentProcess, this, "can't call " + nameof(SetValue) + " on a row with uncommitted staging");

            var hasPreviousValue = _values.TryGetValue(column, out var previousValue);
            if (newValue == null && hasPreviousValue)
            {
                Context.OnRowValueChanged?.Invoke(CurrentProcess, this, new[] { new KeyValuePair<string, object>(column, newValue) });
                _values.Remove(column);
            }
            else if (!hasPreviousValue || newValue != previousValue)
            {
                Context.OnRowValueChanged?.Invoke(CurrentProcess, this, new KeyValuePair<string, object>(column, newValue));
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
                Staging = new Dictionary<string, object>(StringComparer.InvariantCultureIgnoreCase);

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

            Context.OnRowValueChanged?.Invoke(CurrentProcess, this, Staging.ToArray());

            Staging.Clear();
        }
    }
}