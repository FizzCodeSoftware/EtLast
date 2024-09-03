namespace FizzCode.EtLast;

public sealed class ArgumentCollection : IArgumentCollection
{
    public string Instance { get; }
    public IEnumerable<string> AllKeys => _values.Keys;

    public ISecretProvider Secrets { get; private set; }

    private readonly Dictionary<string, object> _values;

    public ArgumentCollection()
    {
        _values = new Dictionary<string, object>(StringComparer.InvariantCultureIgnoreCase);
    }

    public ArgumentCollection(Dictionary<string, object> values)
    {
        _values = new Dictionary<string, object>(values, StringComparer.InvariantCultureIgnoreCase);
    }

    public bool HasKey(string key)
    {
        return _values.ContainsKey(key);
    }

    public T GetAs<T>(string key, T defaultValue = default)
    {
        if (_values.TryGetValue(key, out var value))
        {
            if (value is Func<T> func)
                value = func.Invoke();

            if (value is Func<IArgumentCollection, object> funcWithArgs)
            {
                value = funcWithArgs.Invoke(this);
                _values[key] = value;
            }

            if (value is T castValue)
                return castValue;
        }

        return defaultValue;
    }

    public object Get(string key, object defaultValue = null)
    {
        if (_values.TryGetValue(key, out var value))
        {
            if (value is Func<object> func)
                value = func.Invoke();

            if (value is Func<IArgumentCollection, object> funcWithArgs)
            {
                value = funcWithArgs.Invoke(this);
                _values[key] = value;
            }

            return value;
        }

        return defaultValue;
    }

    public ArgumentCollection(List<IDefaultArgumentProvider> defaultProviders, List<IInstanceArgumentProvider> instanceProviders, string instance, Dictionary<string, string> userArguments)
    {
        Instance = instance;

        var values = new Dictionary<string, object>(StringComparer.InvariantCultureIgnoreCase);
        foreach (var provider in defaultProviders)
        {
            var args = provider.Arguments;
            if (args != null)
            {
                foreach (var kvp in args)
                    values[kvp.Key] = kvp.Value;
            }
        }

        foreach (var provider in instanceProviders.Where(x => string.Equals(x.Instance, instance, StringComparison.InvariantCultureIgnoreCase)))
        {
            var args = provider.Arguments;
            if (args != null)
            {
                foreach (var kvp in args)
                    values[kvp.Key] = kvp.Value;
            }

            if (provider.SecretProvider != null)
                Secrets = provider.SecretProvider;
        }

        if (userArguments != null)
        {
            foreach (var kvp in userArguments)
            {
                values[kvp.Key] = kvp.Value;
            }
        }

        _values = values;
    }
}