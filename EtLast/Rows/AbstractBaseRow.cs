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

        public virtual IEnumerable<KeyValuePair<string, object>> Values { get; }

        public abstract int ColumnCount { get; }

        protected Dictionary<string, object> Staging { get; set; }
        public bool HasStaging => Staging?.Count > 0;

        public object this[string column] => GetValueImpl(column);

        public abstract void SetValue(string column, object newValue);

        protected abstract object GetValueImpl(string column);

        public string ToDebugString()
        {
            return "UID=" + UID.ToString("D", CultureInfo.InvariantCulture) + ", " + string.Join(", ", Values.Select(kvp => kvp.Key + "=" + (kvp.Value != null ? kvp.Value.ToString() + " (" + kvp.Value.GetType().GetFriendlyTypeName() + ")" : "NULL")));
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

            return RowValueComparer.ValuesAreEqual(currentValue, value);
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
            var v = GetValueImpl(column);
            if (v == null)
                return null;

            if (v is string str)
                return str;

            if (v is IFormattable fmt)
                return fmt.ToString(null, CultureInfo.InvariantCulture);

            return v.ToString();
        }

        public virtual void Init(IEtlContext context, IProcess creatorProcess, int uid, IEnumerable<KeyValuePair<string, object>> initialValues)
        {
            Context = context;
            CreatorProcess = creatorProcess;
            CurrentProcess = creatorProcess;
            UID = uid;
        }

        public abstract bool HasValue(string column);

        public abstract void ApplyStaging();

        public abstract void SetStagedValue(string column, object newValue);
    }
}