namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;

    public sealed class EnvironmentSettings : IEnvironmentSettings
    {
        public string Instance { get; }
        public SeqSettings SeqSettings { get; set; } = new SeqSettings();
        public FileLogSettings FileLogSettings { get; set; } = new FileLogSettings();
        public ConsoleLogSettings ConsoleLogSettings { get; set; } = new ConsoleLogSettings();

        public bool IsDevEnvironment { get; set; }
        public TimeSpan TransactionScopeTimeout { get; set; } = TimeSpan.FromMinutes(60);

        public Dictionary<string, Func<IEtlTask>> Commands { get; } = new Dictionary<string, Func<IEtlTask>>(StringComparer.InvariantCultureIgnoreCase);

        private readonly Dictionary<string, object> _configurationValues = new(StringComparer.InvariantCultureIgnoreCase);

        public EnvironmentSettings(string instance, IEnumerable<KeyValuePair<string, object>> configurationValues)
        {
            Instance = instance;
            foreach (var kvp in configurationValues)
            {
                _configurationValues.Add(kvp.Key, kvp.Value);
            }
        }

        public void SetDevEnvironmentForInstance(string instance)
        {
            if (string.Equals(Instance, instance, StringComparison.InvariantCultureIgnoreCase))
                IsDevEnvironment = true;
        }

        public EnvironmentSettings RegisterCommand(string command, Func<IEtlTask> taskCreator)
        {
            Commands.Add(command, taskCreator);
            return this;
        }

        public T GetConfigurationValue<T>(string key, T defaultValue = default)
        {
            if (_configurationValues.TryGetValue(key, out var value) && value is T castValue)
                return castValue;

            return defaultValue;
        }
    }
}