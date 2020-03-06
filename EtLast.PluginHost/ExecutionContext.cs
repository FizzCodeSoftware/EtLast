﻿namespace FizzCode.EtLast.PluginHost
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using FizzCode.EtLast;
    using FizzCode.EtLast.Diagnostics.Interface;
    using Serilog.Events;
    using Serilog.Parsing;

#pragma warning disable CA1001 // Types that own disposable fields should be disposable
    internal class ExecutionContext
#pragma warning restore CA1001 // Types that own disposable fields should be disposable
    {
        public string SessionId { get; }

        public string ModuleName { get; }
        public string PluginName { get; }
        public ITopic Topic { get; set; }
        public IEtlContext Context => Topic.Context;
        public StatCounterCollection CustomCounterCollection { get; set; }
        public string Name { get; }

        public TimeSpan CpuTimeStart { get; private set; }
        public long TotalAllocationsStart { get; private set; }
        public long AllocationDifferenceStart { get; private set; }

        public TimeSpan CpuTimeFinish { get; private set; }
        public long TotalAllocationsFinish { get; private set; }
        public long AllocationDifferenceFinish { get; private set; }

        public TimeSpan RunTime { get; private set; }
        public TimeSpan CpuTime => CpuTimeFinish.Subtract(CpuTimeStart);
        public long TotalAllocations => TotalAllocationsFinish - TotalAllocationsStart;
        public long AllocationDifference => AllocationDifferenceFinish - AllocationDifferenceStart;

        private readonly CommandContext _commandContext;
        private IDiagnosticsSender _diagnosticsSender;

        private readonly object _customFileLock = new object();

        private readonly Dictionary<string, MessageTemplate> _messageTemplateCache = new Dictionary<string, MessageTemplate>();
        private readonly object _messageTemplateCacheLock = new object();
        private readonly MessageTemplateParser _messageTemplateParser = new MessageTemplateParser();

        private Timer _counterSenderTimer;

        public ExecutionContext(string sessionId, IEtlPlugin plugin, Module module, CommandContext commandContext)
        {
            SessionId = sessionId;
            PluginName = plugin?.Name;
            ModuleName = module?.ModuleConfiguration?.ModuleName;

            _commandContext = commandContext;
            Name = PluginName == null
                ? null
                : ModuleName + "/" + PluginName;
        }

        public void ListenToEtlEvents()
        {
            Context.OnLog = Log;
            Context.OnCustomLog = LogCustom;
            Context.OnException = LogException;

            if (_commandContext.HostConfiguration.DiagnosticsUri != null)
            {
                Context.OnRowCreated = LifecycleRowCreated;
                Context.OnRowOwnerChanged = LifecycleRowOwnerChanged;
                Context.OnRowValueChanged = LifecycleRowValueChanged;
                Context.OnRowStoreStarted = LifecycleRowStoreStarted;
                Context.OnRowStored = LifecycleRowStored;
                Context.OnProcessInvocationStart = LifecycleProcessInvocationStart;
                Context.OnProcessInvocationEnd = LifecycleProcessInvocationEnd;
            }

            Context.OnContextIoCommandStart = ContextIoCommandStart;
            Context.OnContextIoCommandEnd = ContextIoCommandEnd;
        }

        public void Log(LogSeverity severity, bool forOps, bool noDiag, string transactionId, IProcess process, string text, params object[] args)
        {
            var sb = new StringBuilder();
            var values = new List<object>();

            if (PluginName != null)
            {
                if (process != null)
                {
                    if (process.Topic?.Name != null)
                    {
                        sb.Append("[{Module}/{Plugin}/{ActiveProcess}/{ActiveTopic}] ");
                        values.Add(ModuleName);
                        values.Add(PluginName);
                        values.Add(process.Name);
                        values.Add(process.Topic?.Name);
                    }
                    else
                    {
                        sb.Append("[{Module}/{Plugin}/{ActiveProcess}] ");
                        values.Add(ModuleName);
                        values.Add(PluginName);
                        values.Add(process.Name);
                    }
                }
                else
                {
                    sb.Append("[{Module}/{Plugin}] ");
                    values.Add(ModuleName);
                    values.Add(PluginName);
                }
            }

            if (transactionId != null)
            {
                sb.Append("/{ActiveTransaction}/ ");
                values.Add(transactionId);
            }

            sb.Append(text);
            if (args != null)
                values.AddRange(args);

            var logger = forOps
                ? _commandContext.OpsLogger
                : _commandContext.Logger;

            logger.Write((LogEventLevel)severity, sb.ToString(), values.ToArray());

            if (!noDiag && (severity >= LogSeverity.Debug) && _diagnosticsSender != null && !string.IsNullOrEmpty(text) && !forOps)
            {
                if (args.Length == 0)
                {
                    _diagnosticsSender.SendDiagnostics(DiagnosticsEventKind.Log, writer =>
                    {
                        writer.Write7BitEncodedInt(_diagnosticsSender.GetTextDictionaryKey(transactionId));
                        writer.Write(text);
                        writer.Write((byte)severity);
                        writer.WriteNullable(process?.InvocationInfo?.InvocationUid);
                        writer.Write7BitEncodedInt(0);
                    });

                    return;
                }

                var template = GetMessageTemplate(text);
                var tokens = template.Tokens.ToList();

                _diagnosticsSender.SendDiagnostics(DiagnosticsEventKind.Log, writer =>
                {
                    writer.Write7BitEncodedInt(_diagnosticsSender.GetTextDictionaryKey(transactionId));
                    writer.Write(text);
                    writer.Write((byte)severity);
                    writer.WriteNullable(process?.InvocationInfo?.InvocationUid);

                    var argCount = 0;
                    for (var i = 0; i < tokens.Count && argCount < args.Length; i++)
                    {
                        if (tokens[i] is PropertyToken pt)
                            argCount++;
                    }

                    writer.Write7BitEncodedInt(argCount);
                    for (int i = 0, idx = 0; i < tokens.Count && idx < args.Length; i++)
                    {
                        if (tokens[i] is PropertyToken pt)
                        {
                            var rawText = text.Substring(pt.StartIndex, pt.Length);
                            writer.Write7BitEncodedInt(_diagnosticsSender.GetTextDictionaryKey(rawText));
                            writer.WriteObject(args[idx]);
                            idx++;
                        }
                    }
                });
            }
        }

        private readonly Dictionary<string, long> _lastCountersSent = new Dictionary<string, long>();

        internal void SendContextCountersToDiagnostics()
        {
            _counterSenderTimer?.Change(Timeout.Infinite, Timeout.Infinite);

            try
            {
                var counters = (CustomCounterCollection ?? Context.CounterCollection).GetCounters();
                if (counters.Count == _lastCountersSent.Count)
                {
                    var same = counters.All(counter => _lastCountersSent.TryGetValue(counter.Code, out var value) && value == counter.Value);
                    if (same)
                        return;
                }

                _diagnosticsSender.SendDiagnostics(DiagnosticsEventKind.ContextCountersUpdated, writer =>
                {
                    writer.Write7BitEncodedInt(counters.Count);
                    foreach (var counter in counters)
                    {
                        writer.Write7BitEncodedInt(_diagnosticsSender.GetTextDictionaryKey(counter.Name));
                        writer.Write(counter.Value);
                        writer.Write((byte)counter.ValueType);
                    }
                });

                foreach (var counter in counters)
                {
                    _lastCountersSent[counter.Code] = counter.Value;
                }
            }
            catch (Exception)
            {
            }
            finally
            {
                _counterSenderTimer?.Change(500, Timeout.Infinite);
            }
        }

        private void LogException(IProcess process, Exception exception)
        {
            var opsErrors = new List<string>();
            GetOpsMessagesRecursive(exception, opsErrors);
            foreach (var opsError in opsErrors)
            {
                Log(LogSeverity.Fatal, true, false, null, process, opsError);
            }

            var msg = exception.FormatExceptionWithDetails();
            Log(LogSeverity.Fatal, false, false, null, process, "{ErrorMessage}", msg);
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
                .Append(Name != null ? (string.Join("\t", Name) + "\t") : "")
                .Append(process != null ? process.Name + "\t" : "")
                .AppendFormat(CultureInfo.InvariantCulture, text, args)
                .ToString();

            lock (_customFileLock)
            {
                File.AppendAllText(filePath, line + Environment.NewLine);
            }
        }

        private void LifecycleRowCreated(IReadOnlyRow row, IProcess process)
        {
            _diagnosticsSender.SendDiagnostics(DiagnosticsEventKind.RowCreated, writer =>
            {
                writer.Write7BitEncodedInt(process.InvocationInfo.InvocationUid);
                writer.Write7BitEncodedInt(row.Uid);
                writer.Write7BitEncodedInt(row.ColumnCount);
                foreach (var kvp in row.Values)
                {
                    writer.Write7BitEncodedInt(_diagnosticsSender.GetTextDictionaryKey(kvp.Key));
                    writer.WriteObject(kvp.Value);
                }
            });
        }

        private void LifecycleRowOwnerChanged(IReadOnlyRow row, IProcess previousProcess, IProcess currentProcess)
        {
            _diagnosticsSender.SendDiagnostics(DiagnosticsEventKind.RowOwnerChanged, writer =>
            {
                writer.Write7BitEncodedInt(row.Uid);
                writer.Write7BitEncodedInt(previousProcess.InvocationInfo.InvocationUid);
                writer.WriteNullable(currentProcess?.InvocationInfo?.InvocationUid);
            });
        }

        private void LifecycleRowStoreStarted(int storeUid, List<KeyValuePair<string, string>> descriptor)
        {
            _diagnosticsSender.SendDiagnostics(DiagnosticsEventKind.RowStoreStarted, writer =>
            {
                writer.Write7BitEncodedInt(storeUid);
                writer.Write7BitEncodedInt(descriptor.Count);
                foreach (var kvp in descriptor)
                {
                    writer.Write7BitEncodedInt(_diagnosticsSender.GetTextDictionaryKey(kvp.Key));
                    writer.Write7BitEncodedInt(_diagnosticsSender.GetTextDictionaryKey(kvp.Value));
                }
            });
        }

        private void LifecycleRowStored(IProcess process, IReadOnlyRow row, int storeUid)
        {
            _diagnosticsSender.SendDiagnostics(DiagnosticsEventKind.RowStored, writer =>
            {
                writer.Write7BitEncodedInt(row.Uid);
                writer.Write7BitEncodedInt(process.InvocationInfo.InvocationUid);
                writer.Write7BitEncodedInt(storeUid);
                writer.Write7BitEncodedInt(row.ColumnCount);
                foreach (var kvp in row.Values)
                {
                    writer.Write7BitEncodedInt(_diagnosticsSender.GetTextDictionaryKey(kvp.Key));
                    writer.WriteObject(kvp.Value);
                }
            });
        }

        private void LifecycleProcessInvocationStart(IProcess process)
        {
            _diagnosticsSender.SendDiagnostics(DiagnosticsEventKind.ProcessInvocationStart, writer =>
            {
                writer.Write7BitEncodedInt(process.InvocationInfo.InvocationUid);
                writer.Write7BitEncodedInt(process.InvocationInfo.InstanceUid);
                writer.Write7BitEncodedInt(process.InvocationInfo.Number);
                writer.Write(process.GetType().GetFriendlyTypeName());
                writer.Write((byte)process.Kind);
                writer.Write(process.Name);
                writer.WriteNullable(process.Topic.Name);
                writer.WriteNullable(process.InvocationInfo.Caller?.InvocationInfo?.InvocationUid);
            });
        }

        private void LifecycleProcessInvocationEnd(IProcess process)
        {
            _diagnosticsSender.SendDiagnostics(DiagnosticsEventKind.ProcessInvocationEnd, writer =>
            {
                writer.Write7BitEncodedInt(process.InvocationInfo.InvocationUid);
                writer.Write(process.InvocationInfo.LastInvocationStarted.ElapsedMilliseconds);
                writer.WriteNullable(process.InvocationInfo.LastInvocationNetTimeMilliseconds);
            });
        }

        private void ContextIoCommandStart(int uid, IoCommandKind kind, string target, IProcess process, int? timeoutSeconds, string command, string transactionId, Func<IEnumerable<KeyValuePair<string, object>>> argumentListGetter, string message, params object[] messageArgs)
        {
            if (message != null)
            {
                var sb = new StringBuilder();
                var values = new List<object>();

                if (PluginName != null)
                {
                    if (process != null)
                    {
                        if (process.Topic?.Name != null)
                        {
                            sb.Append("[{Module}/{Plugin}/{ActiveProcess}/{ActiveTopic}] ");
                            values.Add(ModuleName);
                            values.Add(PluginName);
                            values.Add(process.Name);
                            values.Add(process.Topic?.Name);
                        }
                        else
                        {
                            sb.Append("[{Module}/{Plugin}/{ActiveProcess}] ");
                            values.Add(ModuleName);
                            values.Add(PluginName);
                            values.Add(process.Name);
                        }
                    }
                    else
                    {
                        sb.Append("[{Module}/{Plugin}] ");
                        values.Add(ModuleName);
                        values.Add(PluginName);
                    }
                }

                if (transactionId != null)
                {
                    sb.Append("/{ActiveTransaction}/ ");
                    values.Add(transactionId);
                }

                sb.Append("{IoCommandUid}/{IoCommandKind}");
                values.Add(uid);
                values.Add(kind.ToString());

                if (target != null)
                {
                    sb.Append(", target: {IoCommandTarget}");
                    values.Add(target);
                }

                if (timeoutSeconds != null)
                {
                    sb.Append(", timeout: {IoCommandTimeout}");
                    values.Add(timeoutSeconds);
                }

                sb.Append(", message: ").Append(message);
                if (messageArgs != null)
                    values.AddRange(messageArgs);

                if (command != null)
                {
                    sb.Append(", command: {IoCommand}");
                    values.Add(command);
                }

                _commandContext.IoLogger.Write(LogEventLevel.Verbose, sb.ToString(), values.ToArray());
            }

            _diagnosticsSender?.SendDiagnostics(DiagnosticsEventKind.IoCommandStart, writer =>
            {
                writer.Write7BitEncodedInt(uid);
                writer.Write7BitEncodedInt(process.InvocationInfo.InvocationUid);
                writer.Write((byte)kind);
                writer.Write7BitEncodedInt(_diagnosticsSender.GetTextDictionaryKey(target));
                writer.WriteNullable(timeoutSeconds);
                writer.WriteNullable(command);
                writer.Write7BitEncodedInt(_diagnosticsSender.GetTextDictionaryKey(transactionId));
                var arguments = argumentListGetter?.Invoke()?.ToArray();
                if (arguments?.Length > 0)
                {
                    writer.Write7BitEncodedInt(arguments.Length);
                    foreach (var kvp in arguments)
                    {
                        writer.Write7BitEncodedInt(_diagnosticsSender.GetTextDictionaryKey(kvp.Key));
                        writer.WriteObject(kvp.Value);
                    }
                }
                else
                {
                    writer.Write7BitEncodedInt(0);
                }
            });
        }

        private void ContextIoCommandEnd(IProcess process, int uid, int? affectedDataCount, Exception ex)
        {
            if (ex != null)
            {
                var sb = new StringBuilder();
                var values = new List<object>();

                if (PluginName != null)
                {
                    if (process != null)
                    {
                        if (process.Topic?.Name != null)
                        {
                            sb.Append("[{Module}/{Plugin}/{ActiveProcess}/{ActiveTopic}] ");
                            values.Add(ModuleName);
                            values.Add(PluginName);
                            values.Add(process.Name);
                            values.Add(process.Topic?.Name);
                        }
                        else
                        {
                            sb.Append("[{Module}/{Plugin}/{ActiveProcess}] ");
                            values.Add(ModuleName);
                            values.Add(PluginName);
                            values.Add(process.Name);
                        }
                    }
                    else
                    {
                        sb.Append("[{Module}/{Plugin}] ");
                        values.Add(ModuleName);
                        values.Add(PluginName);
                    }
                }

                sb.Append("{IoCommandUid}/EXCEPTION, {ErrorMessage}");
                values.Add(uid);
                values.Add(ex.FormatExceptionWithDetails());

                _commandContext.IoLogger.Write(LogEventLevel.Error, sb.ToString(), values.ToArray());
            }

            _diagnosticsSender?.SendDiagnostics(DiagnosticsEventKind.IoCommandEnd, writer =>
            {
                writer.Write7BitEncodedInt(uid);
                writer.WriteNullable(affectedDataCount);
                writer.WriteNullable(ex?.FormatExceptionWithDetails());
            });
        }

        private void LifecycleRowValueChanged(IProcess process, IReadOnlyRow row, KeyValuePair<string, object>[] values)
        {
            _diagnosticsSender.SendDiagnostics(DiagnosticsEventKind.RowValueChanged, writer =>
            {
                writer.Write7BitEncodedInt(row.Uid);
                writer.WriteNullable(process?.InvocationInfo?.InvocationUid);

                writer.Write7BitEncodedInt(values.Length);
                foreach (var kvp in values)
                {
                    writer.Write7BitEncodedInt(_diagnosticsSender.GetTextDictionaryKey(kvp.Key));
                    writer.WriteObject(kvp.Value);
                }
            });
        }

        internal void LogCounters()
        {
            var counters = (CustomCounterCollection ?? Context.CounterCollection).GetCounters();

            if (counters.Count == 0)
                return;

            if (PluginName == null)
            {
                Log(LogSeverity.Debug, false, true, null, null, "----------------");
                Log(LogSeverity.Debug, false, true, null, null, "SESSION COUNTERS");
                Log(LogSeverity.Debug, false, true, null, null, "----------------");
            }
            else
            {
                Log(LogSeverity.Debug, false, true, null, null, "---------------");
                Log(LogSeverity.Debug, false, true, null, null, "PLUGIN COUNTERS");
                Log(LogSeverity.Debug, false, true, null, null, "---------------");
            }

            foreach (var counter in counters)
            {
                Log(LogSeverity.Debug, false, true, null, null, "{Counter} = {Value}",
                    counter.Name, counter.TypedValue);
            }
        }

        private Stopwatch _startedOn;

        public void Start()
        {
            _startedOn = Stopwatch.StartNew();

            GC.Collect();
            CpuTimeStart = GetCpuTime();
            TotalAllocationsStart = GetTotalAllocatedBytes();
            AllocationDifferenceStart = GetCurrentAllocatedBytes();

            if (_commandContext.HostConfiguration.DiagnosticsUri != null)
            {
                _diagnosticsSender = new HttpDiagnosticsSender(SessionId, Name, _commandContext.HostConfiguration.DiagnosticsUri);

                _counterSenderTimer = new Timer(timer =>
                {
                    SendContextCountersToDiagnostics();
                }, null, 500, Timeout.Infinite);
            }
        }

        public void Finish()
        {
            _startedOn.Stop();
            RunTime = _startedOn.Elapsed;
            GC.Collect();
            CpuTimeFinish = GetCpuTime();
            TotalAllocationsFinish = GetTotalAllocatedBytes();
            AllocationDifferenceFinish = GetCurrentAllocatedBytes();
        }

        public void Close()
        {
            if (_counterSenderTimer != null)
            {
                _counterSenderTimer.Change(Timeout.Infinite, Timeout.Infinite);
                _counterSenderTimer.Dispose();
                _counterSenderTimer = null;
            }

            if (_diagnosticsSender != null)
            {
                SendContextCountersToDiagnostics();

                _diagnosticsSender.SendDiagnostics(DiagnosticsEventKind.ContextEnded, writer =>
                {
                });

                _diagnosticsSender.Flush();
                _diagnosticsSender.Dispose();
            }
        }

        private static TimeSpan GetCpuTime()
        {
            return AppDomain.CurrentDomain.MonitoringTotalProcessorTime;
        }

        private static long GetCurrentAllocatedBytes()
        {
            return GC.GetTotalMemory(false);
        }

        private static long GetTotalAllocatedBytes()
        {
            return GC.GetTotalAllocatedBytes(true);
        }
    }
}