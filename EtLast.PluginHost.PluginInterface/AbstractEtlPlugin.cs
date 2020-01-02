namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using System.Reflection;
    using FizzCode.DbTools.Configuration;
    using Microsoft.Extensions.Configuration;

    public abstract class AbstractEtlPlugin : IEtlPlugin
    {
        public ModuleConfiguration ModuleConfiguration { get; private set; }
        public IEtlContext Context { get; private set; }

        private IEtlPluginLogger _logger;
        public TimeSpan TransactionScopeTimeout { get; private set; }
        private readonly object _dataLock = new object();

        private string _nameCached;
        public string Name => _nameCached ?? (_nameCached = TypeHelpers.GetFriendlyTypeName(GetType()));

        public void Init(IEtlPluginLogger logger, ModuleConfiguration moduleConfiguration, TimeSpan transactionScopeTimeout, StatCounterCollection moduleStatCounterCollection)
        {
            _logger = logger;
            ModuleConfiguration = moduleConfiguration;
            Context = CreateContext<DictionaryRow>(transactionScopeTimeout, moduleStatCounterCollection);
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

        private IEtlContext CreateContext<TRow>(TimeSpan tansactionScopeTimeout, StatCounterCollection moduleStatCounterCollection)
            where TRow : IRow, new()
        {
            var context = new EtlContext<TRow>(moduleStatCounterCollection)
            {
                TransactionScopeTimeout = tansactionScopeTimeout,
            };

            context.OnException += (sender, args) => _logger.LogException(this, args);
            context.OnLog += (sender, args) => _logger.Log(args.Severity, args.ForOps, this, args.Caller, args.Operation, args.Text, args.Arguments);
            context.OnCustomLog += OnCustomLog;

            return context;
        }

        private void OnCustomLog(object sender, ContextCustomLogEventArgs args)
        {
            var subFolder = args.ForOps ? "log-ops" : "log-dev";
            var logsFolder = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), subFolder);
            if (!Directory.Exists(logsFolder))
            {
                try
                {
                    Directory.CreateDirectory(logsFolder);
                }
                catch (Exception)
                {
                }
            }

            var fileName = Path.Combine(logsFolder, args.FileName);

            var line = TypeHelpers.GetFriendlyTypeName(GetType()) + "\t" + (args.Caller != null ? args.Caller.Name + "\t" : "") + string.Format(CultureInfo.InvariantCulture, args.Text, args.Arguments);

            lock (_dataLock)
            {
                File.AppendAllText(fileName, line + Environment.NewLine);
            }
        }

        protected string GetStorageFolder(params string[] subFolders)
        {
            return GetPathFromConfiguration("StorageFolder", subFolders);
        }

        protected T GetModuleSetting<T>(string key, T defaultValue = default, string subSection = null)
        {
            var section = subSection == null
                ? "Module"
                : "Module:" + subSection;

            var v = ModuleConfiguration.Configuration.GetValue<T>(section + ":" + key + "-" + Environment.MachineName, default);
            if (v != null && !v.Equals(default(T)))
                return v;

            v = ModuleConfiguration.Configuration.GetValue(section + ":" + key, defaultValue);
            return v ?? defaultValue;
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