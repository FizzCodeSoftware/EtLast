namespace FizzCode.EtLast;

public interface IEtlSessionArguments
{
    public IEnumerable<KeyValuePair<string, object>> All { get; }
    public T Get<T>(string key, T defaultValue = default);
}
