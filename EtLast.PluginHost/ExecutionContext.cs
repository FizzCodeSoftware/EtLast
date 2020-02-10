namespace FizzCode.EtLast.PluginHost
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
        public EtlContext Context { get; set; }
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
            Context.OnException = (sender, args) => LogException(args);
            Context.OnLog = Log;
            Context.OnCustomLog = LogCustom;

            if (_commandContext.HostConfiguration.DiagnosticsUri != null)
            {
                Context.OnRowCreated = LifecycleRowCreated;
                Context.OnRowOwnerChanged = LifecycleRowOwnerChanged;
                Context.OnRowValueChanged = LifecycleRowValueChanged;
                Context.OnRowStored = LifecycleRowStored;
                Context.OnProcessInvocationStart = LifecycleProcessInvocationStart;
                Context.OnProcessInvocationEnd = LifecycleProcessInvocationEnd;
                Context.OnContextDataStoreCommand = LifecycleContextDataStoreCommand;
            }
        }

        public void Log(LogSeverity severity, bool forOps, IProcess process, string text, params object[] args)
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

            var values = new List<object>();
            if (PluginName != null)
            {
                values.Add(ModuleName);
                values.Add(PluginName);
            }

            if (process?.Topic != null)
                values.Add(process.Topic);

            if (process != null)
                values.Add(process.Name);

            if (args != null)
                values.AddRange(args);

            var logger = forOps
                ? _commandContext.OpsLogger
                : _commandContext.Logger;

            logger.Write(
                (LogEventLevel)severity,
                (PluginName != null ? "[{Module}/{Plugin}] " : "")
                + (process?.Topic != null ? "[{ActiveTopic}] " : "")
                + (process != null ? ident + "<{ActiveProcess}> " : "")
                + text,
                values.ToArray());

            if (_diagnosticsSender != null)
            {
                if (args.Length == 0)
                {
                    _diagnosticsSender.SendDiagnostics("log", new Diagnostics.Interface.LogEvent()
                    {
                        Timestamp = DateTime.Now.Ticks,
                        Text = text,
                        Severity = severity,
                        ForOps = forOps,
                        ProcessInvocationUID = process?.InvocationUID,
                    });

                    return;
                }

                var template = GetMessageTemplate(text);

                var arguments = new NamedArgument[args.Length];
                var idx = 0;
                var tokens = template.Tokens.ToList();
                for (var i = 0; i < tokens.Count && idx < args.Length; i++)
                {
                    if (tokens[i] is PropertyToken pt)
                    {
                        var rawText = text.Substring(pt.StartIndex, pt.Length);
                        arguments[idx] = NamedArgument.FromObject(rawText, args[idx]);
                        idx++;
                    }
                }

                _diagnosticsSender.SendDiagnostics("log", new Diagnostics.Interface.LogEvent()
                {
                    Timestamp = DateTime.Now.Ticks,
                    Text = text,
                    Severity = severity,
                    ForOps = forOps,
                    ProcessInvocationUID = process?.InvocationUID,
                    Arguments = arguments,
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

                _diagnosticsSender.SendDiagnostics("context-counters-updated", new ContextCountersUpdatedEvent()
                {
                    Timestamp = DateTime.Now.Ticks,
                    Counters = counters.Select(c => new Counter()
                    {
                        Name = c.Name,
                        Value = c.Value,
                        ValueType = c.ValueType,
                    }).ToArray(),
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

        private void LogException(ContextExceptionEventArgs args)
        {
            var opsErrors = new List<string>();
            GetOpsMessagesRecursive(args.Exception, opsErrors);
            foreach (var opsError in opsErrors)
            {
                Log(LogSeverity.Fatal, true, args.Process, opsError);
            }

            var msg = args.Exception.FormatExceptionWithDetails();
            Log(LogSeverity.Fatal, false, args.Process, "{Message}", msg);
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

        private void LifecycleRowCreated(IRow row, IProcess process)
        {
            _diagnosticsSender.SendDiagnostics("row-created", new RowCreatedEvent()
            {
                Timestamp = DateTime.Now.Ticks,
                ProcessInvocationUID = process.InvocationUID,
                RowUid = row.UID,
                Values = row.Values.Select(kvp => NamedArgument.FromObject(kvp.Key, kvp.Value)).ToArray(),
            });
        }

        private void LifecycleRowOwnerChanged(IRow row, IProcess previousProcess, IProcess currentProcess)
        {
            _diagnosticsSender.SendDiagnostics("row-owner-changed", new RowOwnerChangedEvent()
            {
                Timestamp = DateTime.Now.Ticks,
                RowUid = row.UID,
                PreviousProcessInvocationUID = previousProcess.InvocationUID,
                NewProcessInvocationUID = currentProcess?.InvocationUID,
            });
        }

        private void LifecycleRowStored(IProcess process, IRow row, List<KeyValuePair<string, string>> location)
        {
            _diagnosticsSender.SendDiagnostics("row-stored", new RowStoredEvent()
            {
                Timestamp = DateTime.Now.Ticks,
                RowUid = row.UID,
                Locations = location,
                ProcessInvocationUID = process.InvocationUID,
            });
        }

        private void LifecycleProcessInvocationStart(IProcess process)
        {
            _diagnosticsSender.SendDiagnostics("process-invocation-start", new ProcessInvocationStartEvent()
            {
                Timestamp = DateTime.Now.Ticks,
                InvocationUID = process.InvocationUID,
                InstanceUID = process.InstanceUID,
                InvocationCounter = process.InvocationCounter,
                Type = process.GetType().GetFriendlyTypeName(),
                Kind = process.Kind,
                Name = process.Name,
                Topic = process.Topic,
                CallerInvocationUID = process.Caller?.InvocationUID,
            });
        }

        private void LifecycleProcessInvocationEnd(IProcess process)
        {
            _diagnosticsSender.SendDiagnostics("process-invocation-end", new ProcessInvocationEndEvent()
            {
                Timestamp = DateTime.Now.Ticks,
                InvocationUID = process.InvocationUID,
                ElapsedMilliseconds = process.LastInvocationStarted.ElapsedMilliseconds,
            });
        }

        private void LifecycleContextDataStoreCommand(string location, IProcess process, string command, IEnumerable<KeyValuePair<string, object>> args)
        {
            _diagnosticsSender.SendDiagnostics("data-store-command", new DataStoreCommandEvent()
            {
                Timestamp = DateTime.Now.Ticks,
                ProcessInvocationUID = process.InvocationUID,
                Location = location,
                Command = command,
                Arguments = args?.Select(kvp => NamedArgument.FromObject(kvp.Key, kvp.Value)).ToArray(),
            });
        }

        private void LifecycleRowValueChanged(IProcess process, IRow row, IEnumerable<KeyValuePair<string, object>> values)
        {
            _diagnosticsSender.SendDiagnostics("row-value-changed", new RowValueChangedEvent()
            {
                Timestamp = DateTime.Now.Ticks,
                RowUid = row.UID,
                ProcessInvocationUID = process?.InvocationUID,
                Values = values.Select(kvp => NamedArgument.FromObject(kvp.Key, kvp.Value)).ToArray()
            });
        }

        internal void LogCounters()
        {
            var counters = (CustomCounterCollection ?? Context.CounterCollection).GetCounters();

            if (counters.Count == 0)
                return;

            if (PluginName == null)
            {
                Log(LogSeverity.Debug, false, null, "----------------");
                Log(LogSeverity.Debug, false, null, "SESSION COUNTERS");
                Log(LogSeverity.Debug, false, null, "----------------");
            }
            else
            {
                Log(LogSeverity.Debug, false, null, "---------------");
                Log(LogSeverity.Debug, false, null, "PLUGIN COUNTERS");
                Log(LogSeverity.Debug, false, null, "---------------");
            }

            foreach (var counter in counters)
            {
                Log(LogSeverity.Debug, false, null, "{Counter} = {Value}",
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