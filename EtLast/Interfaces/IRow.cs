namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public enum RowState { Normal, Removed, Finished }
    public enum DeferState { None, DeferWait, DeferDone }

    public interface IRow
    {
        IEtlContext Context { get; }
        int UID { get; }
        bool Flagged { get; set; }

        void Init(IEtlContext context, int uid, int columnCountHint = 0); // called right after creation

        void SetValue(string column, object newValue, IProcess process);
        void SetValue(string column, object newValue, IBaseOperation operation);

        void RemoveColumn(string column, IProcess process);
        void RemoveColumn(string column, IBaseOperation operation);

        object this[string column] { get; set; }
        IEnumerable<KeyValuePair<string, object>> Values { get; }

        bool Exists(string column);

        int ColumnCount { get; }

        bool HasError();

        T GetAs<T>(string column);
        T GetAs<T>(string column, T defaultValueIfNull);

        bool IsNull(string column);
        bool IsNullOrEmpty(string column);

        bool IsNullOrEmpty();

        bool IsInt(string column);
        bool IsLong(string column);
        bool IsFloat(string column);
        bool IsDouble(string column);
        bool IsDecimal(string column);

        string ToDebugString();

        IRowOperation CurrentOperation { get; set; }
        RowState State { get; set; }
        DeferState DeferState { get; set; }
    }
}