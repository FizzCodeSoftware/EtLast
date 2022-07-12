namespace FizzCode.EtLast;

public sealed class ArgumentCollection : IArgumentCollection
{
    public IEnumerable<KeyValuePair<string, object>> All => _values;

    private readonly Dictionary<string, object> _values;

    public ArgumentCollection(Dictionary<string, object> values)
    {
        _values = values != null
            ? new Dictionary<string, object>(values, StringComparer.InvariantCultureIgnoreCase)
            : new Dictionary<string, object>(StringComparer.InvariantCultureIgnoreCase);
    }

    public T Get<T>(string key, T defaultValue = default)
    {
        if (_values.TryGetValue(key, out var value))
        {
            if (value is Func<T> func)
                value = func.Invoke();

            if (value is Func<IArgumentCollection, object> funcWithArgs)
                value = funcWithArgs.Invoke(this);

            if (value is T castValue)
                return castValue;
        }

        return defaultValue;
    }

    public ArgumentCollection(List<IDefaultArgumentProvider> defaultProviders, List<IInstanceArgumentProvider> instanceProviders, string instance)
    {
        var argumentValues = new Dictionary<string, object>(StringComparer.InvariantCultureIgnoreCase);

        foreach (var provider in defaultProviders)
        {
            var values = provider.Arguments;
            if (values != null)
            {
                foreach (var kvp in values)
                    argumentValues[kvp.Key] = kvp.Value;
            }
        }

        foreach (var provider in instanceProviders.Where(x => string.Equals(x.Instance, instance, StringComparison.InvariantCultureIgnoreCase)))
        {
            var values = provider.Arguments;
            if (values != null)
            {
                foreach (var kvp in values)
                    argumentValues[kvp.Key] = kvp.Value;
            }
        }

        _values = argumentValues;
    }
}