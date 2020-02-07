namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.Diagnostics;

    [DebuggerDisplay("{" + nameof(ToDebugString) + "()}")]
    public class DictionaryRow : AbstractBaseRow
    {
        private Dictionary<string, object> _values;
        public override IEnumerable<KeyValuePair<string, object>> Values => _values;

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

        protected override void SetValueImpl(string column, object value, IProcess process, IOperation operation)
        {
            Context.OnRowValueChanged?.Invoke(this, column, value, process, operation);

            var hasPreviousValue = _values.TryGetValue(column, out var previousValue);
            if (value == null && hasPreviousValue)
            {
                _values.Remove(column);
            }
            else if (!hasPreviousValue || value != previousValue)
            {
                _values[column] = value;
            }
        }
    }
}