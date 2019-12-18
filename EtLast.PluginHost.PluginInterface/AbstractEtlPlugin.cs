namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using System.Reflection;
    using FizzCode.DbTools.Configuration;
    using Microsoft.Extensions.Configuration;
    using Serilog;
    using Serilog.Events;

    public abstract class AbstractEtlPlugin : IEtlPlugin
    {
        public ModuleConfiguration ModuleConfiguration { get; private set; }
        public IEtlContext Context { get; private set; }

        private ILogger _logger;
        private ILogger _opsLogger;
        public TimeSpan TransactionScopeTimeout { get; private set; }
        private readonly object _dataLock = new object();

        public void Init(ILogger logger, ILogger opsLogger, ModuleConfiguration moduleConfiguration, TimeSpan transactionScopeTimeout, StatCounterCollection moduleStatCounterCollection)
        {
            _logger = logger;
            _opsLogger = opsLogger;
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

            context.OnException += OnException;
            context.OnLog += OnLog;
            context.OnCustomLog += OnCustomLog;

            return context;
        }

        private void OnLog(object sender, ContextLogEventArgs args)
        {
            var ident = "";
            if (args.Caller != null)
            {
                var p = args.Caller;
                while (p.Caller != null)
                {
                    ident += "   ";
                    p = p.Caller;
                }
            }

            if (string.IsNullOrEmpty(ident))
                ident = " ";

            var values = new List<object>
            {
                ModuleConfiguration.ModuleName,
                TypeHelpers.GetFriendlyTypeName(GetType()),
            };

            if (args.Caller != null)
                values.Add(args.Caller.Name);

            if (args.Operation != null)
                values.Add(args.Operation.Name);

            if (args.Arguments != null)
                values.AddRange(args.Arguments);

            var logger = args.ForOps
                ? _opsLogger
                : _logger;

            logger.Write(
                (LogEventLevel)args.Severity,
                "[{Module}/{Plugin}]"
                    + ident
                    + (args.Caller != null ? "<{Caller}> " : "")
                    + (args.Operation != null ? "({Operation}) " : "")
                    + args.Text,
                values.ToArray());
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

        protected virtual void OnException(object sender, ContextExceptionEventArgs args)
        {
            var opsErrors = new List<string>();
            GetOpsMessages(args.Exception, opsErrors);
            foreach (var opsError in opsErrors)
            {
                OnLog(sender, new ContextLogEventArgs()
                {
                    Severity = LogSeverity.Fatal,
                    Caller = args.Process,
                    Text = opsError,
                    ForOps = true,
                });
            }

            var lvl = 0;
            var msg = "EXCEPTION: ";

            var cex = args.Exception;
            while (cex != null)
            {
                if (lvl > 0)
                    msg += "\nINNER EXCEPTION: ";

                msg += TypeHelpers.GetFriendlyTypeName(cex.GetType()) + ": " + cex.Message;

                if (cex.Data?.Count > 0)
                {
                    foreach (var key in cex.Data.Keys)
                    {
                        var k = key.ToString();
                        if (cex == args.Exception && k == "Process")
                            continue;

                        if (k == "CallChain")
                            continue;

                        if (k == "OpsMessage")
                            continue;

                        var value = cex.Data[key];
                        msg += ", " + k + " = " + (value != null ? value.ToString().Trim() : "NULL");
                    }
                }

                cex = cex.InnerException;
                lvl++;
            }

            _logger.Fatal("[{Module}/{Plugin}] " + (args.Process != null ? "<{Process}> " : "") + "{Message}",
                ModuleConfiguration.ModuleName,
                TypeHelpers.GetFriendlyTypeName(GetType()),
                args.Process?.Name,
                msg);
        }

        private void GetOpsMessages(Exception ex, List<string> messages)
        {
            if (ex.Data.Contains(EtlException.OpsMessageDataKey))
            {
                var msg = ex.Data[EtlException.OpsMessageDataKey];
                if (msg != null)
                {
                    messages.Add(msg.ToString());
                }
            }

            if (ex.InnerException != null)
                GetOpsMessages(ex.InnerException, messages);

            if (ex is AggregateException aex)
            {
                foreach (var iex in aex.InnerExceptions)
                {
                    GetOpsMessages(iex, messages);
                }
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