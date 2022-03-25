namespace FizzCode.EtLast;

using System.Collections.Generic;

public interface IEtlSessionArguments
{
    public IEnumerable<KeyValuePair<string, object>> All { get; }
    public T Get<T>(string key, T defaultValue = default);
}
