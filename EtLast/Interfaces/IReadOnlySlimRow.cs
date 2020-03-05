namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;

    public interface IReadOnlySlimRow
    {
        object this[string column] { get; }
        IEnumerable<KeyValuePair<string, object>> Values { get; }
        bool HasValue(string column);
        int ColumnCount { get; }

        bool HasError();

        T GetAs<T>(string column);
        T GetAs<T>(string column, T defaultValueIfNull);

        bool Equals<T>(string column, T value);

        bool IsNull(string column);
        bool IsNullOrEmpty(string column);

        bool IsNullOrEmpty();

        bool Is<T>(string column);
        string FormatToString(string column, IFormatProvider formatProvider = null);
        string GenerateKey(params string[] columns);
        string GenerateKeyUpper(params string[] columns);

        string ToDebugString();
    }
}