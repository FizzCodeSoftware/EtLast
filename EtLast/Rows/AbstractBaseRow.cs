namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;
    using System.Linq;

    [DebuggerDisplay("{" + nameof(ToDebugString) + "()}")]
    public abstract class AbstractBaseRow : IRow
    {
        public IEtlContext Context { get; private set; }
        public IProcess CreatorProcess { get; private set; }
        public IProcess CurrentProcess { get; set; }
        public int UID { get; private set; }

        public IRowOperation CurrentOperation { get; set; }
        public RowState State { get; set; }
        public DeferState DeferState { get; set; }

        public virtual IEnumerable<KeyValuePair<string, object>> Values { get; }

        public abstract int ColumnCount { get; }

        public object this[string column] { get => GetValueImpl(column); set => SetValueImpl(column, value, null, null); }

        public IRow SetValue(string column, object newValue, IProcess process)
        {
            SetValueImpl(column, newValue, process, null);
            return this;
        }

        public IRow SetValue(string column, object newValue, IBaseOperation operation)
        {
            SetValueImpl(column, newValue, operation.Process, operation);
            return this;
        }

        protected abstract object GetValueImpl(string column);
        protected abstract void SetValueImpl(string column, object value, IProcess process, IBaseOperation operation);

        public string ToDebugString()
        {
            return "UID=" + UID.ToString("D", CultureInfo.InvariantCulture) + ", " + string.Join(", ", Values.Select(kvp => kvp.Key + "=" + (kvp.Value != null ? kvp.Value.ToString() + " (" + TypeHelpers.GetFriendlyTypeName(kvp.Value.GetType()) + ")" : "NULL")));
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
                exception.Data.Add("SourceType", TypeHelpers.GetFriendlyTypeName(value?.GetType()));
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
                throw new InvalidCastException("requested cast to '" + TypeHelpers.GetFriendlyTypeName(typeof(T)) + "' is not possible of '" + (value != null ? (value.ToString() + " (" + TypeHelpers.GetFriendlyTypeName(value.GetType()) + ")") : "NULL") + "' in '" + column + "'", ex);
            }
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

        public bool IsInt(string column)
        {
            return GetValueImpl(column) is int;
        }

        public bool IsLong(string column)
        {
            return GetValueImpl(column) is long;
        }

        public bool IsFloat(string column)
        {
            return GetValueImpl(column) is float;
        }

        public bool IsDouble(string column)
        {
            return GetValueImpl(column) is double;
        }

        public bool IsDecimal(string column)
        {
            return GetValueImpl(column) is decimal;
        }

        public virtual void Init(IEtlContext context, IProcess creatorProcess, int uid, IEnumerable<KeyValuePair<string, object>> initialValues)
        {
            Context = context;
            CreatorProcess = creatorProcess;
            CurrentProcess = creatorProcess;
            UID = uid;
        }

        public abstract bool HasValue(string column);
    }
}