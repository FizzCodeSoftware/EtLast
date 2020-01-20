﻿namespace FizzCode.EtLast.PluginHost
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Text;
    using FizzCode.EtLast;
    using Serilog;
    using Serilog.Events;
    using Serilog.Parsing;

    internal class SerilogSessionLogger : ISessionLogger
    {
        public ILogger Logger { get; set; }
        public ILogger OpsLogger { get; set; }
        public IDiagnosticsSender DiagnosticsSender { get; set; }

        private readonly object _customFileLock = new object();

        private readonly Dictionary<string, MessageTemplate> _messageTemplateCache = new Dictionary<string, MessageTemplate>();
        private readonly object _messageTemplateCacheLock = new object();
        private readonly MessageTemplateParser _messageTemplateParser = new MessageTemplateParser();

        public Module CurrentModule { get; private set; }
        public IEtlPlugin CurrentPlugin { get; private set; }

        public void SetCurrentPlugin(Module module, IEtlPlugin plugin)
        {
            CurrentModule = module;
            CurrentPlugin = plugin;
        }

        public void SetupContextEvents(IEtlContext context)
        {
            context.OnException = (sender, args) => LogException(args);
            context.OnLog = Log;
            context.OnCustomLog = LogCustom;
            context.OnRowCreated = LifecycleRowCreated;
            context.OnRowOwnerChanged = LifecycleRowOwnerChanged;
            context.OnRowValueChanged = LifecycleRowValueChanged;
            context.OnRowStored = LifecycleRowStored;
        }

        public void Log(LogSeverity severity, bool forOps, IProcess process, IBaseOperation operation, string text, params object[] args)
        {
            var ident = "";
            if (process != null)
            {
                var p = process;
                while (p.Caller != null)
                {
                    ident += "   ";
                    p = p.Caller;
                }
            }

            if (string.IsNullOrEmpty(ident))
                ident = " ";

            var values = new List<object>();
            if (CurrentModule != null)
                values.Add(CurrentModule.ModuleConfiguration.ModuleName);

            if (CurrentPlugin != null)
                values.Add(CurrentPlugin.Name);

            if (process != null)
                values.Add(process.Name);

            if (operation != null)
                values.Add(operation.Name);

            if (args != null)
                values.AddRange(args);

            var logger = forOps
                ? OpsLogger
                : Logger;

            logger.Write(
                (LogEventLevel)severity,
                (CurrentPlugin != null ? "[{Module}/{Plugin}]" + ident : "")
                + (process != null ? "<{ActiveProcess}> " : "")
                + (operation != null ? "({Operation}) " : "")
                + text,
                values.ToArray());

            if (DiagnosticsSender != null)
            {
                if (args.Length == 0)
                {
                    DiagnosticsSender.SendDiagnostics("log", new Diagnostics.Interface.LogEvent()
                    {
                        Timestamp = DateTime.Now.Ticks,
                        Text = text,
                        Severity = severity,
                        ContextName = CurrentPlugin != null
                            ? new string[] { CurrentModule.ModuleConfiguration.ModuleName, CurrentPlugin.Name }
                            : CurrentModule != null
                                ? new string[] { CurrentModule.ModuleConfiguration.ModuleName }
                                : new string[] { "session" },
                        ForOps = forOps,
                        Process = process == null ? null : new Diagnostics.Interface.ProcessInfo()
                        {
                            Uid = process.UID,
                            Type = TypeHelpers.GetFriendlyTypeName(process.GetType()),
                            Name = process.Name,
                        },
                        Operation = operation == null ? null : new Diagnostics.Interface.OperationInfo()
                        {
                            Number = operation.Number,
                            Type = TypeHelpers.GetFriendlyTypeName(operation.GetType()),
                            Name = operation.Name,
                        }
                    });

                    return;
                }

                var template = GetMessageTemplate(text);

                var arguments = new Diagnostics.Interface.NamedArgument[args.Length];
                var idx = 0;
                var tokens = template.Tokens.ToList();
                for (var i = 0; i < tokens.Count && idx < args.Length; i++)
                {
                    if (tokens[i] is PropertyToken pt)
                    {
                        var rawText = text.Substring(pt.StartIndex, pt.Length);
                        // todo: replace rawText with pt.PropertyName in the original text to remove the unnecessary optional alignment and other attributes
                        arguments[idx] = Diagnostics.Interface.NamedArgument.FromObject(rawText, args[idx]);
                        idx++;
                    }
                }

                DiagnosticsSender.SendDiagnostics("log", new Diagnostics.Interface.LogEvent()
                {
                    Timestamp = DateTime.Now.Ticks,
                    Text = text,
                    Severity = severity,
                    ContextName = CurrentPlugin != null
                            ? new string[] { CurrentModule.ModuleConfiguration.ModuleName, CurrentPlugin.Name }
                            : CurrentModule != null
                                ? new string[] { CurrentModule.ModuleConfiguration.ModuleName }
                                : new string[] { "session" },
                    ForOps = forOps,
                    Process = process == null ? null : new Diagnostics.Interface.ProcessInfo()
                    {
                        Uid = process.UID,
                        Type = TypeHelpers.GetFriendlyTypeName(process.GetType()),
                        Name = process.Name,
                    },
                    Operation = operation == null ? null : new Diagnostics.Interface.OperationInfo()
                    {
                        Number = operation.Number,
                        Type = TypeHelpers.GetFriendlyTypeName(operation.GetType()),
                        Name = operation.Name,
                    },
                    Arguments = arguments,
                });
            }
        }

        private void LogException(ContextExceptionEventArgs args)
        {
            var opsErrors = new List<string>();
            GetOpsMessagesRecursive(args.Exception, opsErrors);
            foreach (var opsError in opsErrors)
            {
                Log(LogSeverity.Fatal, true, args.Process, args.Operation, opsError);
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

            Log(LogSeverity.Fatal, false, args.Process, args.Operation, "{Message}", msg);
        }

        private void GetOpsMessagesRecursive(Exception ex, List<string> messages)
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
                GetOpsMessagesRecursive(ex.InnerException, messages);

            if (ex is AggregateException aex)
            {
                foreach (var iex in aex.InnerExceptions)
                {
                    GetOpsMessagesRecursive(iex, messages);
                }
            }
        }

        private MessageTemplate GetMessageTemplate(string text)
        {
            if (string.IsNullOrEmpty(text))
                return null;

            lock (_messageTemplateCacheLock)
            {
                if (_messageTemplateCache.TryGetValue(text, out var existingTemplate))
                    return existingTemplate;
            }

            var template = _messageTemplateParser.Parse(text);
            lock (_messageTemplateCacheLock)
            {
                if (!_messageTemplateCache.ContainsKey(text))
                {
                    if (_messageTemplateCache.Count == 1000)
                        _messageTemplateCache.Clear();

                    _messageTemplateCache[text] = template;
                }
            }

            return template;
        }

        private void LogCustom(bool forOps, string fileName, IProcess process, string text, params object[] args)
        {
            var logsFolder = forOps
                ? SerilogConfigurator.OpsLogFolder
                : SerilogConfigurator.DevLogFolder;

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

            var filePath = Path.Combine(logsFolder, fileName);

            var line = new StringBuilder()
                .Append(CurrentPlugin != null ? (CurrentPlugin.ModuleConfiguration.ModuleName + "\t" + CurrentPlugin.Name + "\t") : "")
                .Append(process != null ? process.Name + "\t" : "")
                .AppendFormat(CultureInfo.InvariantCulture, text, args)
                .ToString();

            lock (_customFileLock)
            {
                File.AppendAllText(filePath, line + Environment.NewLine);
            }
        }

        private void LifecycleRowCreated(IRow row, IProcess creatorProcess)
        {
            DiagnosticsSender?.SendDiagnostics("row-created", new Diagnostics.Interface.RowCreatedEvent()
            {
                Timestamp = DateTime.Now.Ticks,
                ContextName = CurrentPlugin != null
                            ? new string[] { CurrentModule.ModuleConfiguration.ModuleName, CurrentPlugin.Name }
                            : CurrentModule != null
                                ? new string[] { CurrentModule.ModuleConfiguration.ModuleName }
                                : new string[] { "session" },
                Process = creatorProcess == null ? null : new Diagnostics.Interface.ProcessInfo()
                {
                    Uid = creatorProcess.UID,
                    Type = TypeHelpers.GetFriendlyTypeName(creatorProcess.GetType()),
                    Name = creatorProcess.Name,
                },
                RowUid = row.UID,
                Values = row.Values.Select(x => Diagnostics.Interface.NamedArgument.FromObject(x.Key, x.Value)).ToList(),
            });
        }

        private void LifecycleRowOwnerChanged(IRow row, IProcess previousProcess, IProcess currentProcess)
        {
            DiagnosticsSender?.SendDiagnostics("row-owner-changed", new Diagnostics.Interface.RowOwnerChangedEvent()
            {
                Timestamp = DateTime.Now.Ticks,
                ContextName = CurrentPlugin != null
                            ? new string[] { CurrentModule.ModuleConfiguration.ModuleName, CurrentPlugin.Name }
                            : CurrentModule != null
                                ? new string[] { CurrentModule.ModuleConfiguration.ModuleName }
                                : new string[] { "session" },
                RowUid = row.UID,
                PreviousProcess = previousProcess == null ? null : new Diagnostics.Interface.ProcessInfo()
                {
                    Uid = previousProcess.UID,
                    Type = TypeHelpers.GetFriendlyTypeName(previousProcess.GetType()),
                    Name = previousProcess.Name,
                },
                NewProcess = currentProcess == null ? null : new Diagnostics.Interface.ProcessInfo()
                {
                    Uid = currentProcess.UID,
                    Type = TypeHelpers.GetFriendlyTypeName(currentProcess.GetType()),
                    Name = currentProcess.Name,
                },
            });
        }

        private void LifecycleRowStored(IProcess process, IRowOperation operation, IRow row, List<KeyValuePair<string, string>> location)
        {
            DiagnosticsSender?.SendDiagnostics("row-stored", new Diagnostics.Interface.RowStoredEvent()
            {
                Timestamp = DateTime.Now.Ticks,
                ContextName = CurrentPlugin != null
                            ? new string[] { CurrentModule.ModuleConfiguration.ModuleName, CurrentPlugin.Name }
                            : CurrentModule != null
                                ? new string[] { CurrentModule.ModuleConfiguration.ModuleName }
                                : new string[] { "session" },
                RowUid = row.UID,
                Locations = location,
                Process = process == null ? null : new Diagnostics.Interface.ProcessInfo()
                {
                    Uid = process.UID,
                    Type = TypeHelpers.GetFriendlyTypeName(process.GetType()),
                    Name = process.Name,
                },
                Operation = operation == null ? null : new Diagnostics.Interface.OperationInfo()
                {
                    Number = operation.Number,
                    Type = TypeHelpers.GetFriendlyTypeName(operation.GetType()),
                    Name = operation.Name,
                },
            });
        }

        private void LifecycleRowValueChanged(IRow row, string column, object previousValue, object currentValue, IProcess process, IBaseOperation operation)
        {
            DiagnosticsSender?.SendDiagnostics("row-value-changed", new Diagnostics.Interface.RowValueChangedEvent()
            {
                Timestamp = DateTime.Now.Ticks,
                ContextName = CurrentPlugin != null
                            ? new string[] { CurrentModule.ModuleConfiguration.ModuleName, CurrentPlugin.Name }
                            : CurrentModule != null
                                ? new string[] { CurrentModule.ModuleConfiguration.ModuleName }
                                : new string[] { "session" },
                RowUid = row.UID,
                Column = column,
                PreviousValue = Diagnostics.Interface.Argument.FromObject(previousValue),
                CurrentValue = Diagnostics.Interface.Argument.FromObject(currentValue),
                ProcessUid = process?.UID,
                ProcessType = process != null ? TypeHelpers.GetFriendlyTypeName(process.GetType()) : null,
                ProcessName = process?.Name,
                OperationName = operation?.Name,
                OperationType = operation != null ? TypeHelpers.GetFriendlyTypeName(operation.GetType()) : null,
                OperationNumber = operation?.Number,
            });
        }
    }
}