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
            Context.OnRowCreated = LifecycleRowCreated;
            Context.OnRowOwnerChanged = LifecycleRowOwnerChanged;
            Context.OnRowValueChanged = LifecycleRowValueChanged;
            Context.OnRowStored = LifecycleRowStored;
            Context.OnProcessCreated = LifecycleProcessCreated;
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
            if (PluginName != null)
            {
                values.Add(ModuleName);
                values.Add(PluginName);
            }

            if (process != null)
                values.Add(process.Name);

            if (operation != null)
                values.Add(operation.Name);

            if (args != null)
                values.AddRange(args);

            var logger = forOps
                ? _commandContext.OpsLogger
                : _commandContext.Logger;

            logger.Write(
                (LogEventLevel)severity,
                (PluginName != null ? "[{Module}/{Plugin}]" + ident : "")
                + (process != null ? "<{ActiveProcess}> " : "")
                + (operation != null ? "({Operation}) " : "")
                + text,
                values.ToArray());

            if (_diagnosticsSender != null)
            {
                if (args.Length == 0)
                {
                    _diagnosticsSender.SendDiagnostics("log", new Diagnostics.Interface.LogEvent()
                    {
                        Ts = DateTime.Now.Ticks,
                        Text = text,
                        Severity = severity,
                        ForOps = forOps,
                        ProcessUid = process?.UID,
                        Operation = operation == null ? null : new OperationInfo()
                        {
                            Number = operation.Number,
                            Type = operation.GetType().GetFriendlyTypeName(),
                            Name = operation.Name,
                        }
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
                    Ts = DateTime.Now.Ticks,
                    Text = text,
                    Severity = severity,
                    ForOps = forOps,
                    ProcessUid = process?.UID,
                    Operation = operation == null ? null : new OperationInfo()
                    {
                        Number = operation.Number,
                        Type = operation.GetType().GetFriendlyTypeName(),
                        Name = operation.Name,
                    },
                    Arguments = arguments,
                });
            }
        }

        private readonly Dictionary<string, long> _lastCountersSent = new Dictionary<string, long>();

        internal void SendContextCountersToDiagnostics()
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
                Ts = DateTime.Now.Ticks,
                Counters = counters.Select(c => new Counter()
                {
                    Name = c.Name,
                    Code = c.Code,
                    Value = c.Value,
                    ValueType = c.ValueType,
                }).ToList(),
            });

            foreach (var counter in counters)
            {
                _lastCountersSent[counter.Code] = counter.Value;
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

            var msg = args.Exception.FormatExceptionWithDetails();
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
                .Append(Name != null ? (string.Join("\t", Name) + "\t") : "")
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
            _diagnosticsSender?.SendDiagnostics("row-created", new RowCreatedEvent()
            {
                Ts = DateTime.Now.Ticks,
                ProcessUid = creatorProcess.UID,
                RowUid = row.UID,
                Values = row.Values.Select(x => NamedArgument.FromObject(x.Key, x.Value)).ToList(),
            });
        }

        private void LifecycleRowOwnerChanged(IRow row, IProcess previousProcess, IProcess currentProcess)
        {
            _diagnosticsSender?.SendDiagnostics("row-owner-changed", new RowOwnerChangedEvent()
            {
                Ts = DateTime.Now.Ticks,
                RowUid = row.UID,
                PreviousProcessUid = previousProcess.UID,
                NewProcessUid = currentProcess?.UID,
            });
        }

        private void LifecycleRowStored(IProcess process, IRowOperation operation, IRow row, List<KeyValuePair<string, string>> location)
        {
            _diagnosticsSender?.SendDiagnostics("row-stored", new RowStoredEvent()
            {
                Ts = DateTime.Now.Ticks,
                RowUid = row.UID,
                Locations = location,
                ProcessUid = process.UID,
                Operation = operation == null ? null : new OperationInfo()
                {
                    Number = operation.Number,
                    Type = operation.GetType().GetFriendlyTypeName(),
                    Name = operation.Name,
                },
            });
        }

        private void LifecycleProcessCreated(int uid, IProcess process)
        {
            _diagnosticsSender?.SendDiagnostics("process-created", new ProcessCreatedEvent()
            {
                Ts = DateTime.Now.Ticks,
                Uid = uid,
                Type = process.GetType().GetFriendlyTypeName(),
                Name = process.Name,
            });
        }

        private void LifecycleRowValueChanged(IRow row, string column, object currentValue, IProcess process, IBaseOperation operation)
        {
            _diagnosticsSender?.SendDiagnostics("row-value-changed", new RowValueChangedEvent()
            {
                Ts = DateTime.Now.Ticks,
                RowUid = row.UID,
                Column = column,
                CurrentValue = Argument.FromObject(currentValue),
                ProcessUid = process?.UID,
                OperationName = operation?.Name,
                OperationType = operation?.GetType().GetFriendlyTypeName(),
                OperationNumber = operation?.Number,
            });
        }

        internal void LogCounters()
        {
            var counters = (CustomCounterCollection ?? Context.CounterCollection).GetCounters();

            if (counters.Count == 0)
                return;

            if (PluginName == null)
            {
                Log(LogSeverity.Information, false, null, null, "----------------");
                Log(LogSeverity.Information, false, null, null, "SESSION COUNTERS");
                Log(LogSeverity.Information, false, null, null, "----------------");
            }
            else
            {
                Log(LogSeverity.Information, false, null, null, "---------------");
                Log(LogSeverity.Information, false, null, null, "PLUGIN COUNTERS");
                Log(LogSeverity.Information, false, null, null, "---------------");
            }

            foreach (var counter in counters)
            {
                Log(LogSeverity.Information, false, null, null, "{Counter} = {Value}",
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
                }, null, 500, 500);
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