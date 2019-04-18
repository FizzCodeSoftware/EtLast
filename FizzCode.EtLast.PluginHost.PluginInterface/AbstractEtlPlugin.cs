﻿namespace FizzCode.EtLast
{
    using Serilog;
    using Serilog.Events;
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.IO;
    using System.Reflection;

    public abstract class AbstractEtlPlugin : IEtlPlugin
    {
        public Configuration Configuration { get; private set; }
        public EtlPluginResult PluginResult { get; private set; }
        public string PluginFolder { get; private set; }
        private static readonly Dictionary<LogSeverity, LogEventLevel> _logEventLevelMap;
        public ILogger Logger { get; private set; }
        public ILogger OpsLogger { get; private set; }

        static AbstractEtlPlugin()
        {
            _logEventLevelMap = new Dictionary<LogSeverity, LogEventLevel>()
            {
                [LogSeverity.Verbose] = LogEventLevel.Verbose,
                [LogSeverity.Debug] = LogEventLevel.Debug,
                [LogSeverity.Information] = LogEventLevel.Information,
                [LogSeverity.Warning] = LogEventLevel.Warning,
                [LogSeverity.Error] = LogEventLevel.Error,
            };
        }

        public void Init(ILogger logger, ILogger opsLogger, Configuration configuration, EtlPluginResult pluginResult, string pluginFolder)
        {
            Logger = logger;
            OpsLogger = opsLogger;
            Configuration = configuration;
            PluginResult = pluginResult;
            PluginFolder = pluginFolder;
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

            if (string.IsNullOrEmpty(ident)) ident = " ";

            var values = new List<object>
            {
                GetType().Name,
            };

            if (args.Process != null) values.Add(args.Process.Name);
            if (args.Arguments != null) values.AddRange(args.Arguments);

            var valuesArray = values.ToArray();

            var logger = args.ForOps ? OpsLogger : Logger;
            logger.Write(_logEventLevelMap[args.Severity], "{@Plugin}" + ident + (args.Process != null ? "{@Process} " : "") + args.Text, valuesArray);
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

            Logger.Write(_logEventLevelMap[LogSeverity.Error], args.Exception,
                "{Plugin}, " + (args.Process != null ? "{Process} " : "") + "{Message}",
                GetType().Name,
                args.Process?.Name,
                args.Exception.Message);
            // append exception to regular log file
            /*OnLog(sender, new ContextLogEventArgs()
            {
                Severity = LogSeverity.Error,
                Process = args.Process,
                Text = args.Exception.ToString(),
            });*/
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

            if (ex.InnerException != null) GetOpsMessages(ex.InnerException, messages);

            if (ex is AggregateException aex)
            {
                foreach (var iex in aex.InnerExceptions)
                {
                    GetOpsMessages(iex, messages);
                }
            }
        }

        protected EtlPluginResult ExecuteWrapper(IEtlContext context, Action action, bool terminatePluginScopeOnFail, bool terminateGlobalScopeOnFail)
        {
            try
            {
                action.Invoke();

                var exceptions = context.GetExceptions();
                if (exceptions.Count > 0)
                {
                    return new EtlPluginResult()
                    {
                        Success = false,
                        TerminatePluginScope = terminatePluginScopeOnFail,
                        TerminateGlobalScope = terminateGlobalScopeOnFail,
                        Exceptions = new List<Exception>(exceptions),
                    };
                }

                return new EtlPluginResult()
                {
                    Success = true,
                };
            }
            catch (Exception unhandledException)
            {
                var result = new EtlPluginResult()
                {
                    Success = false,
                    TerminatePluginScope = terminatePluginScopeOnFail,
                    TerminateGlobalScope = terminateGlobalScopeOnFail,
                    Exceptions = new List<Exception>(context.GetExceptions()),
                };

                result.Exceptions.Add(unhandledException);
                return result;
            }
        }

        protected string GetStorageFolder(params string[] subFolders)
        {
            return GetPathFromConfiguration("StorageFolder", subFolders);
        }

        protected string GetPathFromConfiguration(string appSettingName, params string[] subFolders)
        {
            var path = Configuration.AppSettings.Settings[appSettingName].Value;
            if (string.IsNullOrEmpty(path)) return null;

            if (path.StartsWith(@".\"))
            {
                var exeFolder = Path.GetDirectoryName(Assembly.GetEntryAssembly().Location);
                path = Path.Combine(exeFolder, path.Substring(2));
            }

            if (subFolders != null && subFolders.Length > 0)
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