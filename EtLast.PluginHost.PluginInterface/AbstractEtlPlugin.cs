namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Reflection;
    using FizzCode.DbTools.Configuration;
    using Microsoft.Extensions.Configuration;

    public abstract class AbstractEtlPlugin : IEtlPlugin
    {
        private string _nameCached;
        public string Name => _nameCached ?? (_nameCached = TypeHelpers.GetFriendlyTypeName(GetType()));

        public ModuleConfiguration ModuleConfiguration { get; private set; }
        public IEtlContext Context { get; private set; }

        protected IEtlPluginLogger Logger { get; private set; }
        protected TimeSpan TransactionScopeTimeout { get; private set; }

        public void Init(IEtlPluginLogger logger, ModuleConfiguration moduleConfiguration, TimeSpan transactionScopeTimeout, StatCounterCollection moduleStatCounterCollection)
        {
            Logger = logger;
            ModuleConfiguration = moduleConfiguration;

            Context = new EtlContext<DictionaryRow>(moduleStatCounterCollection)
            {
                TransactionScopeTimeout = transactionScopeTimeout,
                OnException = (sender, args) => Logger.LogException(this, args),
                OnLog = (sender, args) => Logger.Log(args.Severity, args.ForOps, this, args.Caller, args.Operation, args.Text, args.Arguments),
                OnCustomLog = (sender, args) => Logger.LogCustom(args.ForOps, this, args.FileName, args.Caller, args.Text, args.Arguments),
            };

            TransactionScopeTimeout = transactionScopeTimeout;
        }

        public void BeforeExecute()
        {
            CustomBeforeExecute();
        }

        protected virtual void CustomBeforeExecute()
        {
        }

        public void AfterExecute()
        {
            CustomAfterExecute();
            LogCounters();
        }

        private void LogCounters()
        {
            var counters = Context.CounterCollection.GetCounters();
            if (counters.Count == 0)
                return;

            foreach (var counter in counters)
            {
                Context.Log(counter.IsDebug ? LogSeverity.Debug : LogSeverity.Information, null, "counter {Counter} = {Value}", counter.Name, counter.TypedValue);
            }
        }

        protected virtual void CustomAfterExecute()
        {
        }

        public abstract void Execute();

        protected string GetStorageFolder(params string[] subFolders)
        {
            return GetPathFromConfiguration("StorageFolder", subFolders);
        }

        protected T GetModuleSetting<T>(string key, T defaultValue = default, string subSection = null)
        {
            var v = ModuleConfiguration.Configuration.GetValue<T>((subSection == null ? "Module" : "Module:" + subSection) + ":" + key + "-" + Environment.MachineName, default);
            if (v != null && !v.Equals(default(T)))
                return v;

            v = ModuleConfiguration.Configuration.GetValue((subSection == null ? "Module" : "Module:" + subSection) + ":" + key, defaultValue);
            if (v != null && !v.Equals(default(T)))
                return v;

            v = ModuleConfiguration.Configuration.GetValue<T>((subSection == null ? "Shared" : "Shared:" + subSection) + ":" + key + "-" + Environment.MachineName, default);
            if (v != null && !v.Equals(default(T)))
                return v;

            v = ModuleConfiguration.Configuration.GetValue<T>((subSection == null ? "Shared" : "Shared:" + subSection) + ":" + key, default);
            if (v != null && !v.Equals(default(T)))
                return v;

            return defaultValue;
        }

        protected string GetPathFromConfiguration(string appSettingName, params string[] subFolders)
        {
            var path = GetModuleSetting<string>(appSettingName);
            if (string.IsNullOrEmpty(path))
                return null;

            if (path.StartsWith(@".\", StringComparison.InvariantCultureIgnoreCase))
            {
                var exeFolder = Path.GetDirectoryName(Assembly.GetEntryAssembly().Location);
                path = Path.Combine(exeFolder, path.Substring(2));
            }

            if (subFolders?.Length > 0)
            {
                var l = new List<string>() { path };
                l.AddRange(subFolders);
                path = Path.Combine(l.ToArray());
            }

            if (!Directory.Exists(path))
            {
                Directory.CreateDirectory(path);
            }

            return path;
        }

        protected string GetFileNameFromConfiguration(string appSettingName)
        {
            var fileName = GetModuleSetting<string>(appSettingName);
            if (string.IsNullOrEmpty(fileName))
                return null;

            if (fileName.StartsWith(@".\", StringComparison.InvariantCultureIgnoreCase))
            {
                var exeFolder = Path.GetDirectoryName(Assembly.GetEntryAssembly().Location);
                fileName = Path.Combine(exeFolder, fileName.Substring(2));
            }

            return fileName;
        }

        protected virtual ConnectionStringWithProvider GetConnectionString(string key, bool allowMachineNameOverride = true)
        {
            if (ModuleConfiguration.ConnectionStrings == null)
                return null;

            if (allowMachineNameOverride)
            {
                var connectionString = ModuleConfiguration.ConnectionStrings[key + "-" + Environment.MachineName];
                if (connectionString != null)
                    return connectionString;
            }

            return ModuleConfiguration.ConnectionStrings[key];
        }
    }
}