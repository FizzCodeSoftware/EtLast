namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public interface ICountableLookup
    {
        int Count { get; }
        IEnumerable<string> Keys { get; }
        void AddRow(string key, IRow row);
        void Clear();
        int GetRowCountByKey(string key);
    }
}