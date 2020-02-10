namespace FizzCode.EtLast
{
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
                ? new Dictionary<string, object>()
                : new Dictionary<string, object>(initialValues);
        }

        protected override object GetValueImpl(string column)
        {
            return _values.TryGetValue(column, out var value)
                ? value
                : null;
        }

        public override void SetValue(IProcess process, string column, object newValue)
        {
            var hasPreviousValue = _values.TryGetValue(column, out var previousValue);
            if (newValue == null && hasPreviousValue)
            {
                Context.OnRowValueChanged?.Invoke(process, this, new[] { new KeyValuePair<string, object>(column, newValue) });
                _values.Remove(column);
            }
            else if (!hasPreviousValue || newValue != previousValue)
            {
                Context.OnRowValueChanged?.Invoke(process, this, new[] { new KeyValuePair<string, object>(column, newValue) });
                _values[column] = newValue;
            }
        }

        public override void ApplyStaging(IProcess process)
        {
            if (ValueStackInternal == null || ValueStackInternal.Count == 0)
                return;

            var changes = ValueStackInternal.Where(kvp =>
            {
                var hasPreviousValue = _values.TryGetValue(kvp.Key, out var previousValue);
                return (!hasPreviousValue && kvp.Value != null) || (hasPreviousValue && kvp.Value != previousValue);
            }).ToArray();

            ValueStackInternal.Clear();

            foreach (var kvp in changes)
            {
                var hasPreviousValue = _values.TryGetValue(kvp.Key, out var previousValue);
                if (kvp.Value == null)
                {
                    _values.Remove(kvp.Key);
                }
                else
                {
                    _values[kvp.Key] = kvp.Value;
                }
            }

            Context.OnRowValueChanged?.Invoke(process, this, changes);
        }
    }
}