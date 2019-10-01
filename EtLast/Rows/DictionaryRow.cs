namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.Diagnostics;

    [DebuggerDisplay("{" + nameof(ToDebugString) + "()}")]
    public class DictionaryRow : AbstractBaseRow, IRow
    {
        private Dictionary<string, object> _values;
        public override IEnumerable<KeyValuePair<string, object>> Values => _values;

        public bool Exists(string column)
        {
            return _values.ContainsKey(column);
        }

        public int ColumnCount => _values.Count;

        public void Init(IEtlContext context, int uid, int columnCountHint = 0)
        {
            Context = context;
            UID = uid;
            _values = columnCountHint == 0 ? new Dictionary<string, object>() : new Dictionary<string, object>(columnCountHint);
        }

        protected override object InternalGetValue(string column)
        {
            return _values.TryGetValue(column, out var value) ? value : null;
        }

        protected override void InternalSetValue(string column, object value, IProcess process, IBaseOperation operation)
        {
            if (Flagged)
            {
                if (value != null)
                {
                    if (operation != null)
                    {
                        Context.LogRow(process, this, "column {Column} set to ({ValueType}) {Value} by ({OperationName})", column, TypeHelpers.GetFriendlyTypeName(value.GetType()), value, operation.Name);
                    }
                    else
                    {
                        Context.LogRow(process, this, "column {Column} set to ({ValueType}) {Value}", column, TypeHelpers.GetFriendlyTypeName(value.GetType()), value);
                    }
                }
                else if (operation != null)
                {
                    Context.LogRow(process, this, "column {Column} set to NULL by ({OperationName})", column, operation.Name);
                }
                else
                {
                    Context.LogRow(process, this, "column {Column} set to NULL", column);
                }
            }

            _values[column] = value;
        }

        protected override void InternalRemoveColumn(string column, IProcess process, IBaseOperation operation)
        {
            if (Flagged)
            {
                Context.LogRow(null, this, "column {Column} removed", column);
            }

            _values.Remove(column);
        }
    }
}