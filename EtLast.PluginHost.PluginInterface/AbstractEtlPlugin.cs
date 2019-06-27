namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using Serilog;
    using Serilog.Events;

    public abstract class AbstractEtlPlugin : IEtlPlugin
    {
        public Configuration Configuration { get; private set; }
        public EtlPluginResult PluginResult { get; private set; }
        public string PluginFolder { get; private set; }
        private static readonly Dictionary<LogSeverity, LogEventLevel> LogEventLevelMap;
        public ILogger Logger { get; private set; }
        public ILogger OpsLogger { get; private set; }
        public TimeSpan TransactionScopeTimeout { get; private set; }

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

        public void Init(ILogger logger, ILogger opsLogger, Configuration configuration, EtlPluginResult pluginResult, string pluginFolder, TimeSpan transactionScopeTimeout)
        {
            Logger = logger;
            OpsLogger = opsLogger;
            Configuration = configuration;
            PluginResult = pluginResult;
            PluginFolder = pluginFolder;
            TransactionScopeTimeout = transactionScopeTimeout;
        }

        public abstract void Execute();

        public IEtlContext CreateContext<TRow>()
            where TRow : IRow, new()
        {
            var context = new EtlContext<TRow>(Configuration);
            context.OnException += OnException;
            context.OnLog += OnLog;

            context.Log(LogSeverity.Information, null, "ETL context created by {UserName}", !string.IsNullOrWhiteSpace(Environment.UserDomainName) ? $@"{Environment.UserDomainName}\{Environment.UserName}" : Environment.UserName);

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

        /// <summary>
        /// Sequentially executes the specified wrappers in the specified order.
        /// If a wrapper fails then the execution will stop and return to the caller.
        /// </summary>
        /// <param name="context">The context to be used.</param>
        /// <param name="terminateHostOnFail">If true, then a failed wrapper will set the <see cref="EtlPluginResult.TerminateHost"/> field to true in the result object.</param>
        /// <param name="wrappers">The wrappers to be executed.</param>
        /// <returns></returns>
        protected EtlPluginResult Execute(IEtlContext context, bool terminateHostOnFail, params IEtlWrapper[] wrappers)
        {
            var initialExceptionCount = context.GetExceptions().Count;

            try
            {
                foreach (var wrapper in wrappers)
                {
                    wrapper.Execute(context, TransactionScopeTimeout);

                    var exceptions = context.GetExceptions();
                    if (exceptions.Count > initialExceptionCount)
                        break;
                }

                var finalExceptions = context.GetExceptions();
                var result = finalExceptions.Count > initialExceptionCount
                    ? new EtlPluginResult()
                    {
                        Success = false,
                        TerminateHost = terminateHostOnFail,
                        Exceptions = new List<Exception>(finalExceptions.Skip(initialExceptionCount)),
                    }
                    : new EtlPluginResult()
                    {
                        Success = true,
                    };

                PluginResult.MergeWith(result);

                return result;
            }
            catch (Exception unhandledException)
            {
                var exceptions = context.GetExceptions();
                var result = new EtlPluginResult()
                {
                    Success = false,
                    TerminateHost = terminateHostOnFail,
                    Exceptions = new List<Exception>(exceptions.Skip(initialExceptionCount)),
                };

                result.Exceptions.Add(unhandledException);

                PluginResult.MergeWith(result);

                return result;
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
            var path = Configuration.AppSettings.Settings[appSettingName].Value;
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