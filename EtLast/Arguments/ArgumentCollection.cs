namespace FizzCode.EtLast;

public sealed class ArgumentCollection : IArgumentCollection
{
    public IEnumerable<string> AllKeys => _values.Keys;

    private readonly List<ISecretProvider> _secretProviders = [];

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

    public string GetSecret(string name)
    {
        foreach (var provider in _secretProviders)
        {
            try
            {
                var value = provider.Get(name);
                if (!string.IsNullOrEmpty(value))
                    return value;
            }
            catch (Exception)
            {
            }
        }

        return null;
    }

    public ArgumentCollection(List<ArgumentProvider> defaultProviders, List<InstanceArgumentProvider> instanceProviders, Dictionary<string, string> userArguments, Dictionary<string, object> overrides)
    {
        var values = new Dictionary<string, object>(StringComparer.InvariantCultureIgnoreCase);
        foreach (var provider in defaultProviders)
        {
            var args = provider.CreateArguments(this);
            if (args != null)
            {
                foreach (var kvp in args)
                    values[kvp.Key] = kvp.Value;
            }
        }

        foreach (var provider in instanceProviders.Where(x => string.Equals(x.Instance, Environment.MachineName, StringComparison.InvariantCultureIgnoreCase)))
        {
            var args = provider.CreateArguments(this);
            if (args != null)
            {
                foreach (var kvp in args)
                    values[kvp.Key] = kvp.Value;
            }

            var sp = provider.CreateSecretProvider();
            if (sp != null)
                _secretProviders.Add(sp);
        }

        if (userArguments != null)
        {
            foreach (var kvp in userArguments)
            {
                values[kvp.Key] = kvp.Value;
            }
        }

        if (overrides != null)
        {
            foreach (var kvp in overrides)
            {
                values[kvp.Key] = kvp.Value;
            }
        }

        _values = values;
    }

    public ArgumentCollection(ArgumentCollection baseArgumentCollection, List<ArgumentProvider> defaultProviders, List<InstanceArgumentProvider> instanceProviders, Dictionary<string, string> userArguments, Dictionary<string, object> overrides)
    {
        var values = new Dictionary<string, object>(baseArgumentCollection._values, StringComparer.InvariantCultureIgnoreCase);
        foreach (var provider in defaultProviders)
        {
            var args = provider.CreateArguments(this);
            if (args != null)
            {
                foreach (var kvp in args)
                    values[kvp.Key] = kvp.Value;
            }
        }

        foreach (var provider in instanceProviders.Where(x => string.Equals(x.Instance, Environment.MachineName, StringComparison.InvariantCultureIgnoreCase)))
        {
            var args = provider.CreateArguments(this);
            if (args != null)
            {
                foreach (var kvp in args)
                    values[kvp.Key] = kvp.Value;
            }

            var sp = provider.CreateSecretProvider();
            if (sp != null)
                _secretProviders.Add(sp);
        }

        if (userArguments != null)
        {
            foreach (var kvp in userArguments)
            {
                values[kvp.Key] = kvp.Value;
            }
        }

        if (overrides != null)
        {
            foreach (var kvp in overrides)
            {
                values[kvp.Key] = kvp.Value;
            }
        }

        _values = values;
    }

    public ArgumentCollection(ArgumentCollection baseArgumentCollection, Dictionary<string, object> overrides)
    {
        var values = new Dictionary<string, object>(baseArgumentCollection._values, StringComparer.InvariantCultureIgnoreCase);

        if (overrides != null)
        {
            foreach (var kvp in overrides)
            {
                values[kvp.Key] = kvp.Value;
            }
        }

        _values = values;
    }
}