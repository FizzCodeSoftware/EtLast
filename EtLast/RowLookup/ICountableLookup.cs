namespace FizzCode.EtLast
{
    public interface ICountableLookup
    {
        int Count { get; }
        void AddRow(string key, IRow row);
        void Clear();
        int GetRowCountByKey(string key);
    }
}