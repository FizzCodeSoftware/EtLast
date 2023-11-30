namespace FizzCode.EtLast;

public interface IArgumentCollection
{
    string Instance { get; }
    IEnumerable<string> AllKeys { get; }
    T GetAs<T>(string key, T defaultValue = default);
    object Get(string key, object defaultValue = default);
}