namespace FizzCode.EtLast.PluginHost
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;
    using System.IO;
    using System.Text;
    using FizzCode.EtLast;
    using FizzCode.LightWeight.Configuration;
    using Microsoft.Extensions.Configuration;
    using Serilog.Events;

#pragma warning disable CA1001 // Types that own disposable fields should be disposable
    internal class ExecutionContext : IExecutionContext, IEtlContextListener
#pragma warning restore CA1001 // Types that own disposable fields should be disposable
    {
        public string SessionId { get; }
        public IExecutionContext ParentContext { get; }
        public ITopic Topic { get; }
        public string Name { get; }

        public string ModuleName { get; }
        public string PluginName { get; }

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

        private readonly object _customFileLock = new object();

        public Dictionary<IoCommandKind, IoCommandCounter> IoCommandCounters { get; } = new Dictionary<IoCommandKind, IoCommandCounter>();

        public List<IEtlContextListener> CustomListeners { get; } = new List<IEtlContextListener>();

        public ExecutionContext(ExecutionContext parentContext, ITopic topic, string sessionId, IEtlPlugin plugin, Module module, CommandContext commandContext)
        {
            SessionId = sessionId;
            Topic = topic;
            ParentContext = parentContext;
            PluginName = plugin?.Name;
            ModuleName = module?.ModuleConfiguration?.ModuleName;

            _commandContext = commandContext;
            Name = PluginName == null
                ? null
                : ModuleName + "/" + PluginName;
        }

        public void OnLog(LogSeverity severity, bool forOps, string transactionId, IProcess process, string text, params object[] args)
        {
            Log(severity, forOps, transactionId, process, text, args);

            foreach (var listener in CustomListeners)
                listener.OnLog(severity, forOps, transactionId, process, text, args);
        }

        private void Log(LogSeverity severity, bool forOps, string transactionId, IProcess process, string text, params object[] args)
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
        }

        public void OnException(IProcess process, Exception exception)
        {
            var opsErrors = new List<string>();
            GetOpsMessagesRecursive(exception, opsErrors);
            foreach (var opsError in opsErrors)
            {
                OnLog(LogSeverity.Fatal, true, null, process, opsError);
            }

            var msg = exception.FormatExceptionWithDetails();
            Log(LogSeverity.Fatal, false, null, process, "{ErrorMessage}", msg);

            foreach (var listener in CustomListeners)
                listener.OnException(process, exception);
        }

        public void GetOpsMessagesRecursive(Exception ex, List<string> messages)
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

        public void OnCustomLog(bool forOps, string fileName, IProcess process, string text, params object[] args)
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
                .Append(!string.IsNullOrEmpty(process?.Topic?.Name)
                    ? process.Topic.Name + "\t"
                    : "")
                .Append(process != null
                    ? process.Name + "\t"
                    : "")
                .AppendFormat(CultureInfo.InvariantCulture, text, args)
                .ToString();

            lock (_customFileLock)
            {
                File.AppendAllText(filePath, line + Environment.NewLine);
            }

            foreach (var listener in CustomListeners)
                listener.OnCustomLog(forOps, fileName, process, text, args);
        }

        public void OnRowCreated(IReadOnlyRow row, IProcess process)
        {
            foreach (var listener in CustomListeners)
                listener.OnRowCreated(row, process);
        }

        public void OnRowOwnerChanged(IReadOnlyRow row, IProcess previousProcess, IProcess currentProcess)
        {
            foreach (var listener in CustomListeners)
                listener.OnRowOwnerChanged(row, previousProcess, currentProcess);
        }

        public void OnRowStoreStarted(int storeUid, string location, string path)
        {
            foreach (var listener in CustomListeners)
                listener.OnRowStoreStarted(storeUid, location, path);
        }

        public void OnRowStored(IProcess process, IReadOnlyRow row, int storeUid)
        {
            foreach (var listener in CustomListeners)
                listener.OnRowStored(process, row, storeUid);
        }

        public void OnProcessInvocationStart(IProcess process)
        {
            foreach (var listener in CustomListeners)
                listener.OnProcessInvocationStart(process);
        }

        public void OnProcessInvocationEnd(IProcess process)
        {
            foreach (var listener in CustomListeners)
                listener.OnProcessInvocationEnd(process);
        }

        public void OnContextIoCommandStart(int uid, IoCommandKind kind, string location, string path, IProcess process, int? timeoutSeconds, string command, string transactionId, Func<IEnumerable<KeyValuePair<string, object>>> argumentListGetter, string message, params object[] messageArgs)
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

                if (location != null)
                {
                    sb.Append(", location: {IoCommandTarget}");
                    values.Add(location);
                }

                if (path != null)
                {
                    sb.Append(", path: {IoCommandTargetPath}");
                    values.Add(path);
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

            foreach (var listener in CustomListeners)
                listener.OnContextIoCommandStart(uid, kind, location, path, process, timeoutSeconds, command, transactionId, argumentListGetter, message, messageArgs);
        }

        public void OnContextIoCommandEnd(IProcess process, int uid, IoCommandKind kind, int? affectedDataCount, Exception ex)
        {
            IoCommandCounters.TryGetValue(kind, out var counter);
            if (counter == null)
            {
                IoCommandCounters[kind] = counter = new IoCommandCounter();
            }

            counter.InvocationCount++;

            if (affectedDataCount != null)
            {
                var cnt = (counter.AffectedDataCount ?? 0) + affectedDataCount.Value;
                counter.AffectedDataCount = cnt;
            }

            if (ParentContext is ExecutionContext pec)
            {
                pec.IoCommandCounters.TryGetValue(kind, out counter);
                if (counter == null)
                {
                    pec.IoCommandCounters[kind] = counter = new IoCommandCounter();
                }

                counter.InvocationCount++;

                if (affectedDataCount != null)
                {
                    var cnt = (counter.AffectedDataCount ?? 0) + affectedDataCount.Value;
                    counter.AffectedDataCount = cnt;
                }
            }

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

            foreach (var listener in CustomListeners)
                listener.OnContextIoCommandEnd(process, uid, kind, affectedDataCount, ex);
        }

        public void OnRowValueChanged(IProcess process, IReadOnlyRow row, KeyValuePair<string, object>[] values)
        {
            foreach (var listener in CustomListeners)
                listener.OnRowValueChanged(process, row, values);
        }

        private Stopwatch _startedOn;

        public void Start()
        {
            _startedOn = Stopwatch.StartNew();

            var listeners = _commandContext.HostConfiguration.GetEtlContextListeners(this);
            if (listeners?.Count > 0)
            {
                CustomListeners.AddRange(listeners);
            }

            GC.Collect();
            CpuTimeStart = GetCpuTime();
            TotalAllocationsStart = GetTotalAllocatedBytes();
            AllocationDifferenceStart = GetCurrentAllocatedBytes();
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

        public void OnContextClosed()
        {
            foreach (var listener in CustomListeners)
                listener.OnContextClosed();
        }

        public bool Init(IExecutionContext executionContext, IConfigurationSection configurationSection, IConfigurationSecretProtector configurationSecretProtector)
        {
            throw new NotImplementedException();
        }

        public class IoCommandCounter
        {
            public int InvocationCount { get; set; }
            public int? AffectedDataCount { get; set; }
        }
    }
}