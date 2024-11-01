namespace FizzCode.EtLast;

public interface IArgumentCollection
{
    public IEnumerable<string> AllKeys { get; }
    public T GetAs<T>(string key, T defaultValue = default);
    public object Get(string key, object defaultValue = default);
    public bool HasKey(string key);

    public string GetSecret(string name);
    public void Inject(object target, string scopeName, HashSet<string> excludedPropertyNames = null, bool overwriteArguments = false);
}