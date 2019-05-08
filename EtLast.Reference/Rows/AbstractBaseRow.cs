namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;
    using System.Linq;

    [DebuggerDisplay("{" + nameof(ToDebugString) + "()}")]
    public abstract class AbstractBaseRow
    {
        public IEtlContext Context { get; protected set; }
        public int UID { get; protected set; }
        public bool Flagged { get; set; }

        public IRowOperation CurrentOperation { get; set; }
        public RowState State { get; set; }

        public virtual IEnumerable<KeyValuePair<string, object>> Values { get; }

        public object this[string column] { get => InternalGetValue(column); set => InternalSetValue(column, value, null, null); }

        public void SetValue(string column, object value, IProcess process)
        {
            InternalSetValue(column, value, process, null);
        }

        public void SetValue(string column, object value, IBaseOperation operation)
        {
            InternalSetValue(column, value, operation.Process, operation);
        }

        protected abstract object InternalGetValue(string column);
        protected abstract void InternalSetValue(string column, object value, IProcess process, IBaseOperation operation);
        protected abstract void InternalRemoveColumn(string column, IProcess process, IBaseOperation operation);

        public void RemoveColumn(string column, IProcess process)
        {
            InternalRemoveColumn(column, process, null);
        }

        public void RemoveColumn(string column, IBaseOperation operation)
        {
            InternalRemoveColumn(column, operation.Process, operation);
        }

        public string ToDebugString()
        {
            return "row " + UID.ToString("D", CultureInfo.InvariantCulture) + (Flagged ? " (flagged)" : string.Empty) + " // " + string.Join(Environment.NewLine, Values.Select(kvp => "[" + kvp.Key + "] = " + (kvp.Value != null ? "(" + kvp.Value.GetType().Name + ") " + kvp.Value.ToString() : "NULL")));
        }

        /// <summary>
        /// Returns true if any value is <see cref="EtlRowError"/>.
        /// </summary>
        /// <returns>True if any value is <see cref="EtlRowError"/>.</returns>
        public bool HasError()
        {
            return Values.Any(x => x.Value is EtlRowError);
        }

        public T GetAs<T>(string column)
        {
            var value = InternalGetValue(column);
            try
            {
                return (T)value;
            }
            catch (Exception ex)
            {
                var exception = new InvalidCastException("error raised during a cast operation", ex);
                exception.Data.Add("Column", column);
                exception.Data.Add("Value", value != null ? value.ToString() : "NULL");
                exception.Data.Add("SourceType", value?.GetType().Name);
                exception.Data.Add("TargetType", typeof(T).Name);
                throw exception;
            }
        }

        public T GetAs<T>(string column, T defaultValueIfNull)
        {
            var value = InternalGetValue(column);
            if (value == null)
                return defaultValueIfNull;
            try
            {
                return (T)value;
            }
            catch (Exception ex)
            {
                throw new InvalidCastException("requested cast to '" + typeof(T).Name + "' is not possible of '" + (value != null ? (value.ToString() + " (" + value.GetType().Name + ")") : "NULL") + "' in '" + column + "'", ex);
            }
        }

        public bool IsNull(string column)
        {
            var value = InternalGetValue(column);
            return value == null;
        }

        public bool IsNullOrEmpty(string column)
        {
            var value = InternalGetValue(column);
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

        public bool IsInt(string column)
        {
            return InternalGetValue(column) is int;
        }

        public bool IsLong(string column)
        {
            return InternalGetValue(column) is long;
        }

        public bool IsFloat(string column)
        {
            return InternalGetValue(column) is float;
        }

        public bool IsDouble(string column)
        {
            return InternalGetValue(column) is double;
        }

        public bool IsDecimal(string column)
        {
            return InternalGetValue(column) is decimal;
        }
    }
}