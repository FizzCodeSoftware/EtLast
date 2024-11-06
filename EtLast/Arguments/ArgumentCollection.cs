using System.Reflection;

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

    public void Add(string key, object value)
    {
        _values.Add(key, value);
    }

    public void Add(string key, Func<object> valueFunc)
    {
        _values.Add(key, valueFunc);
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
            if (value is Func<object> valueFunc)
                value = valueFunc.Invoke();

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

    public T CreateAndInject<T>(string scopeName = null, HashSet<string> excludedPropertyNames = null, bool overwriteArguments = false)
        where T : new()
    {
        var target = new T();
        Inject(target, scopeName, excludedPropertyNames, overwriteArguments);
        return target;
    }

    public void Inject(object target, string scopeName = null, HashSet<string> excludedPropertyNames = null, bool overwriteArguments = false)
    {
        var properties = target.GetType().GetProperties(BindingFlags.Instance | BindingFlags.SetProperty | BindingFlags.Public)
            .Where(p => p.SetMethod?.IsPrivate == false && (excludedPropertyNames?.Contains(p.Name) != true) && p.GetIndexParameters().Length == 0)
            .ToList();

        foreach (var property in properties)
        {
            var argumentKey = AllKeys
                .FirstOrDefault(x => string.Equals(x, property.Name, StringComparison.InvariantCultureIgnoreCase)
                                  || string.Equals(x, "!" + property.Name, StringComparison.InvariantCultureIgnoreCase));

            if (scopeName != null)
            {
                argumentKey ??= AllKeys
                    .FirstOrDefault(x => string.Equals(x, scopeName + ":" + property.Name, StringComparison.InvariantCultureIgnoreCase)
                                      || string.Equals(x, "!" + scopeName + ":" + property.Name, StringComparison.InvariantCultureIgnoreCase));
            }

            if (argumentKey == null || !HasKey(argumentKey))
                continue;

            var overwrite = overwriteArguments || argumentKey.StartsWith('!');

            if (!overwrite)
            {
                var existingValue = property.GetValue(target);
                if (existingValue != null)
                {
                    if (existingValue.GetType().IsValueType)
                    {
                        var defaultValue = Activator.CreateInstance(existingValue.GetType());
                        if (!existingValue.Equals(defaultValue))
                            continue;
                    }
                    else
                    {
                        continue;
                    }
                }
            }

            try
            {
                var argumentValue = Get(argumentKey);
                if (argumentValue != null)
                {
                    var argumentType = argumentValue.GetType();
                    if (property.PropertyType.IsAssignableFrom(argumentType))
                    {
                        property.SetValue(target, argumentValue);
                    }
                    else
                    {
                        object convertedArgumentValue = null;
                        if (argumentValue is string argumentValueAsString)
                        {
                            argumentValueAsString = argumentValueAsString.Trim();

                            if (property.PropertyType == typeof(int))
                            {
                                if (int.TryParse(argumentValueAsString, CultureInfo.InvariantCulture, out var v))
                                    convertedArgumentValue = v;
                            }
                            else if (property.PropertyType == typeof(uint))
                            {
                                if (uint.TryParse(argumentValueAsString, CultureInfo.InvariantCulture, out var v))
                                    convertedArgumentValue = v;
                            }
                            if (property.PropertyType == typeof(short))
                            {
                                if (short.TryParse(argumentValueAsString, CultureInfo.InvariantCulture, out var v))
                                    convertedArgumentValue = v;
                            }
                            else if (property.PropertyType == typeof(ushort))
                            {
                                if (ushort.TryParse(argumentValueAsString, CultureInfo.InvariantCulture, out var v))
                                    convertedArgumentValue = v;
                            }
                            else if (property.PropertyType == typeof(long))
                            {
                                if (long.TryParse(argumentValueAsString, CultureInfo.InvariantCulture, out var v))
                                    convertedArgumentValue = v;
                            }
                            else if (property.PropertyType == typeof(ulong))
                            {
                                if (ulong.TryParse(argumentValueAsString, CultureInfo.InvariantCulture, out var v))
                                    convertedArgumentValue = v;
                            }
                            else if (property.PropertyType == typeof(bool))
                            {
                                convertedArgumentValue =
                                       argumentValueAsString.Equals("true", StringComparison.InvariantCultureIgnoreCase)
                                    || argumentValueAsString == "1"
                                    || argumentValueAsString == "yes"
                                    || argumentValueAsString == "on"
                                    || argumentValueAsString == "enabled"
                                    || argumentValueAsString == "allowed"
                                    || argumentValueAsString == "active";
                            }
                        }

                        if (convertedArgumentValue != null)
                        {
                            property.SetValue(target, convertedArgumentValue);
                        }
                        else
                        {
                            var exception = new Exception("property '" + property.Name + "' (" + property.PropertyType.GetFriendlyTypeName() + ") is not assignable to argument value type " + argumentType.GetFriendlyTypeName());
                            exception.Data["argument"] = argumentKey;
                            throw exception;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                var exception = new Exception("can't resolve arguments for type " + target.GetType().GetFriendlyTypeName(), ex);
                exception.Data["argument"] = argumentKey;
                throw exception;
            }
        }
    }
}