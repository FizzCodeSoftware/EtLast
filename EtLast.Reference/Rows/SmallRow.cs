namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;

    [DebuggerDisplay("{" + nameof(ToDebugString) + "()}")]
    public class SmallRow : AbstractBaseRow, IRow
    {
        private struct Entry
        {
            public string Name;
            public object Value;
        }

        private Entry[] _items;

        public override IEnumerable<KeyValuePair<string, object>> Values
        {
            get
            {
                for (var i = 0; i < ColumnCount; i++) yield return new KeyValuePair<string, object>(_items[i].Name, _items[i].Value);
            }
        }

        public bool Exists(string column) => _items.Any(x => x.Name == column);

        public int ColumnCount { get; private set; } = 0;

        public void Init(IEtlContext context, int uid, int columnCountHint = 0)
        {
            Context = context;
            UID = uid;
            _items = new Entry[columnCountHint];
            ColumnCount = 0;
        }

        protected override object InternalGetValue(string column)
        {
            for (var i = 0; i < ColumnCount; i++)
                if (_items[i].Name == column) return _items[i].Value;

            return null;
        }

        protected override void InternalSetValue(string column, object value, IProcess process, IBaseOperation operation)
        {
            if (Flagged)
            {
                if (value != null)
                {
                    if (operation != null)
                    {
                        Context.LogRow(process, this, "column {Column} set to ({ValueType}) {Value} by {OperationName}", column, value.GetType().Name, value, operation.Name);
                    }
                    else
                    {
                        Context.LogRow(process, this, "column {Column} set to ({ValueType}) {Value}", column, value.GetType().Name, value);
                    }
                }
                else
                {
                    if (operation != null)
                    {
                        Context.LogRow(process, this, "column {Column} set to NULL by {OperationName}", column, operation.Name);
                    }
                    else
                    {
                        Context.LogRow(process, this, "column {Column} set to NULL", column);
                    }
                }
            }

            for (var i = 0; i < ColumnCount; i++)
            {
                if (_items[i].Name == column)
                {
                    _items[i].Value = value;
                    return;
                }
            }

            if (ColumnCount == _items.Length)
            {
                var newItems = new Entry[ColumnCount + 1];
                Array.Copy(_items, 0, newItems, 0, ColumnCount);
                _items = newItems;
            }

            _items[ColumnCount].Name = column;
            _items[ColumnCount].Value = value;
            ColumnCount++;
        }

        protected override void InternalRemoveColumn(string column, IProcess process, IBaseOperation operation)
        {
            for (var i = 0; i < _items.Length; i++)
            {
                if (_items[i].Name == column)
                {
                    if (Flagged)
                    {
                        if (operation != null)
                        {
                            Context.LogRow(operation.Process, this, "column {Column} removed by {OperationName}", column, operation.Name);
                        }
                        else
                        {
                            Context.LogRow(null, this, "column {Column} removed", column);
                        }
                    }

                    ColumnCount--;
                    if (i < ColumnCount)
                    {
                        Array.Copy(_items, i + 1, _items, i, ColumnCount - i);
                    }

                    _items[ColumnCount].Name = null;
                    _items[ColumnCount].Value = null;
                }
            }
        }
    }
}