namespace FizzCode.EtLast;

public interface IArgumentCollection
{
    public IEnumerable<KeyValuePair<string, object>> All { get; }
    public T Get<T>(string key, T defaultValue = default);
}