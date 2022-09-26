namespace FizzCode.EtLast;

public interface IArgumentCollection
{
    public IEnumerable<string> AllKeys { get; }
    public T GetAs<T>(string key, T defaultValue = default);
    public object Get(string key, object defaultValue = default);
}