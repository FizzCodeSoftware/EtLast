namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;

    public interface IEnvironmentSettings
    {
        public SeqSettings SeqSettings { get; }
        public FileLogSettings FileLogSettings { get; }
        public ConsoleLogSettings ConsoleLogSettings { get; }

        public bool IsDevEnvironment { get; }
        public TimeSpan TransactionScopeTimeout { get; }

        public Dictionary<string, Func<IEtlTask>> Commands { get; }

        public void SetDevEnvironmentForInstance(string instance);
        public EnvironmentSettings RegisterCommand(string command, Func<IEtlTask> taskCreator);

        public T GetConfigurationValue<T>(string key, T defaultValue = default);
    }
}