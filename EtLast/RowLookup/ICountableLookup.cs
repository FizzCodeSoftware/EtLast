namespace FizzCode.EtLast;

public interface ICountableLookup
{
    int Count { get; }
    IEnumerable<string> Keys { get; }
    void AddRow(string key, IReadOnlySlimRow row);
    void Clear();
    int CountByKey(string key);
}