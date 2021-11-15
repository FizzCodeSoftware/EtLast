namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;

    public interface IReadOnlySlimRow
    {
        object this[string column] { get; }
        IEnumerable<KeyValuePair<string, object>> Values { get; }
        int ColumnCount { get; }
        object Tag { get; }

        bool HasError();

        T GetAs<T>(string column);
        T GetAs<T>(string column, T defaultValueIfNull);

        bool Equals<T>(string column, T value);

        bool HasValue(string column);
        bool IsNullOrEmpty(string column);

        bool IsNullOrEmpty();

        bool Is<T>(string column);
        string FormatToString(string column, IFormatProvider formatProvider = null);
        string GenerateKey(params string[] columns);
        string GenerateKeyUpper(params string[] columns);

        string ToDebugString(bool multiLine = false);
    }
}