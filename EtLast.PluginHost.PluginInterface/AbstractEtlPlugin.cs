namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.IO;
    using System.Reflection;
    using Serilog;
    using Serilog.Events;

    public abstract class AbstractEtlPlugin : IEtlPlugin
    {
        public Configuration Configuration { get; private set; }
        public IEtlContext Context { get; private set; }
        public string ModuleFolder { get; private set; }
        private static readonly Dictionary<LogSeverity, LogEventLevel> LogEventLevelMap;
        public ILogger Logger { get; private set; }
        public ILogger OpsLogger { get; private set; }
        public TimeSpan TransactionScopeTimeout { get; private set; }
        private readonly object _dataLock = new object();

        static AbstractEtlPlugin()
        {
            LogEventLevelMap = new Dictionary<LogSeverity, LogEventLevel>()
            {
                [LogSeverity.Verbose] = LogEventLevel.Verbose,
                [LogSeverity.Debug] = LogEventLevel.Debug,
                [LogSeverity.Information] = LogEventLevel.Information,
                [LogSeverity.Warning] = LogEventLevel.Warning,
                [LogSeverity.Error] = LogEventLevel.Error,
            };
        }

        public void Init(ILogger logger, ILogger opsLogger, Configuration configuration, string moduleFolder, TimeSpan transactionScopeTimeout)
        {
            Logger = logger;
            OpsLogger = opsLogger;
            Configuration = configuration;
            Context = CreateContext<DictionaryRow>(transactionScopeTimeout);
            ModuleFolder = moduleFolder;
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
            LogExceptions();
            LogStats();
        }

        private void LogStats()
        {
            var counters = Context.Stat.GetCountersOrdered();
            if (counters.Count == 0)
                return;

            foreach (var kvp in counters)
            {
                Context.Log(LogSeverity.Information, null, "stat {StatName} = {StatValue}", kvp.Key, kvp.Value);
            }
        }

        private void LogExceptions()
        {
            if (Context.Result.Exceptions.Count > 0)
            {
                Logger.Write(LogEventLevel.Error, "{ExceptionCount} exceptions raised during plugin execution", Context.Result.Exceptions.Count);
                OpsLogger.Write(LogEventLevel.Error, "{ExceptionCount} exceptions raised during plugin execution", Context.Result.Exceptions.Count);

                var index = 0;
                foreach (var ex in Context.Result.Exceptions)
                {
                    Logger.Write(LogEventLevel.Error, ex, "exception #{ExceptionIndex}", index++);

                    var opsMsg = ex.Message;
                    if (ex.Data.Contains(EtlException.OpsMessageDataKey) && (ex.Data[EtlException.OpsMessageDataKey] != null))
                    {
                        opsMsg = ex.Data[EtlException.OpsMessageDataKey].ToString();
                    }

                    OpsLogger.Write(LogEventLevel.Error, "exception #{ExceptionIndex}: {Message}", index++, opsMsg);
                }
            }
        }

        protected virtual void CustomAfterExecute()
        {
        }

        public abstract void Execute();

        private IEtlContext CreateContext<TRow>(TimeSpan tansactionScopeTimeout)
            where TRow : IRow, new()
        {
            var context = new EtlContext<TRow>(Configuration)
            {
                TransactionScopeTimeout = tansactionScopeTimeout
            };

            context.OnException += OnException;
            context.OnLog += OnLog;
            context.OnCustomLog += OnCustomLog;

            //context.Log(LogSeverity.Information, null, "ETL context created by {UserName}", !string.IsNullOrWhiteSpace(Environment.UserDomainName) ? $@"{Environment.UserDomainName}\{Environment.UserName}" : Environment.UserName);

            return context;
        }

        private void OnLog(object sender, ContextLogEventArgs args)
        {
            var ident = string.Empty;
            if (args.Process != null)
            {
                var p = args.Process;
                while (p.Caller != null)
                {
                    ident += "\t";
                    p = p.Caller;
                }
            }

            if (string.IsNullOrEmpty(ident))
                ident = " ";

            var values = new List<object>
            {
                GetType().Name,
            };

            if (args.Process != null)
                values.Add(args.Process.Name);
            if (args.Arguments != null)
                values.AddRange(args.Arguments);

            var valuesArray = values.ToArray();

            var logger = args.ForOps ? OpsLogger : Logger;
            logger.Write(LogEventLevelMap[args.Severity], "{@Plugin}" + ident + (args.Process != null ? "{@Process} " : "") + args.Text, valuesArray);
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

            var line = string.Format(GetType().Name + "\t" + (args.Process != null ? args.Process.Name + "\t" : "") + args.Text, args.Arguments);

            lock (_dataLock)
            {
                File.AppendAllText(fileName, line);
            }
        }

        protected virtual void OnException(object sender, ContextExceptionEventArgs args)
        {
            var opsErrors = new List<string>();
            GetOpsMessages(args.Exception, opsErrors);
            foreach (var msg in opsErrors)
            {
                OnLog(sender, new ContextLogEventArgs()
                {
                    Severity = LogSeverity.Error,
                    Process = args.Process,
                    Text = msg,
                    ForOps = true,
                });
            }

            Logger.Write(LogEventLevelMap[LogSeverity.Error], args.Exception,
                "{Plugin}, " + (args.Process != null ? "{Process} " : "") + "{Message}",
                GetType().Name,
                args.Process?.Name,
                args.Exception.Message);
        }

        public void GetOpsMessages(Exception ex, List<string> messages)
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

        protected string GetAppSetting(string key)
        {
            return Configuration.AppSettings.Settings[key + "-" + Environment.MachineName] != null
                ? Configuration.AppSettings.Settings[key + "-" + Environment.MachineName].Value
                : Configuration.AppSettings.Settings[key]?.Value;
        }

        protected string GetPathFromConfiguration(string appSettingName, params string[] subFolders)
        {
            var path = GetAppSetting(appSettingName);
            if (string.IsNullOrEmpty(path))
                return null;

            if (path.StartsWith(@".\"))
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
    }
}