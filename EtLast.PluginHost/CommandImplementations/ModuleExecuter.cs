namespace FizzCode.EtLast.PluginHost
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;
    using System.Transactions;
    using FizzCode.EtLast;

    internal static class ModuleExecuter
    {
        public static ExecutionResult Execute(CommandContext commandContext, params Module[] modules)
        {
            var result = ExecutionResult.Success;

            var sessionId = Guid.NewGuid().ToString("N", CultureInfo.InvariantCulture).Substring(0, 6);

            ISessionLogger sessionLogger = new SerilogSessionLogger()
            {
                Logger = commandContext.Logger,
                OpsLogger = commandContext.OpsLogger,
                DiagnosticsSender = commandContext.HostConfiguration.DiagnosticsUri != null
                    ? new HttpDiagnosticsSender(sessionId, commandContext.HostConfiguration.DiagnosticsUri)
                    : null,
            };

            GC.Collect();

            var sessionStartedOn = Stopwatch.StartNew();
            var sessionStartCpuTime = GetCpuTime();
            var sessionStartTotalAllocatedBytes = GetTotalAllocatedBytes();
            var sessionStartCurrentAllocatedBytes = GetCurrentAllocatedBytes();
            var sessionCounterCollection = new StatCounterCollection();
            var sessionWarningCount = 0;
            var sessionExceptionCount = 0;

            var pluginInformation = new List<SessionPluginInfo>();
            foreach (var module in modules)
            {
                foreach (var plugin in module.EnabledPlugins)
                {
                    pluginInformation.Add(new SessionPluginInfo()
                    {
                        Module = module,
                        Plugin = plugin,
                    });
                }
            }

            try
            {
                sessionLogger.Log(LogSeverity.Information, false, null, null, "session {SessionId} started", sessionId);

                foreach (var pluginInfo in pluginInformation)
                {
                    var pluginStartedOn = Stopwatch.StartNew();
                    //var friendlyPluginName = TypeHelpers.GetFriendlyTypeName(pluginInfo.Plugin.GetType());

                    pluginInfo.Context = new EtlContext(sessionCounterCollection)
                    {
                        TransactionScopeTimeout = commandContext.HostConfiguration.TransactionScopeTimeout,
                    };

                    sessionLogger.SetCurrentPlugin(pluginInfo.Module, pluginInfo.Plugin);
                    sessionLogger.SetupContextEvents(pluginInfo.Context);

                    sessionLogger.Log(LogSeverity.Information, false, null, null, "plugin started");

                    GC.Collect();
                    pluginInfo.CpuTimeStart = GetCpuTime();
                    pluginInfo.TotalAllocationsStart = GetTotalAllocatedBytes();
                    pluginInfo.AllocationDifferenceStart = GetCurrentAllocatedBytes();
                    try
                    {
                        try
                        {
                            pluginInfo.Plugin.Init(pluginInfo.Context, pluginInfo.Module.ModuleConfiguration);
                            pluginInfo.Plugin.BeforeExecute();
                            pluginInfo.Plugin.Execute();

                            sessionWarningCount += pluginInfo.Context.Result.WarningCount;
                            sessionExceptionCount += pluginInfo.Context.Result.Exceptions.Count;

                            if (pluginInfo.Context.Result.TerminateHost)
                            {
                                sessionLogger.Log(LogSeverity.Error, false, null, null, "requested to terminate the execution of the module");

                                result = ExecutionResult.PluginFailedAndExecutionTerminated;

                                pluginStartedOn.Stop();
                                pluginInfo.RunTime = pluginStartedOn.Elapsed;
                                GC.Collect();
                                pluginInfo.CpuTimeFinish = GetCpuTime();
                                pluginInfo.TotalAllocationsFinish = GetTotalAllocatedBytes();
                                pluginInfo.AllocationDifferenceFinish = GetCurrentAllocatedBytes();
                                break; // stop processing plugins
                            }

                            if (!pluginInfo.Context.Result.Success)
                            {
                                result = ExecutionResult.PluginFailed;
                            }
                        }
                        catch (Exception ex)
                        {
                            result = ExecutionResult.PluginFailedAndExecutionTerminated;

                            sessionLogger.Log(LogSeverity.Error, false, null, null, "unhandled error during plugin execution after {Elapsed}", pluginStartedOn.Elapsed);
                            sessionLogger.Log(LogSeverity.Error, true, null, null, "requested to terminate the execution of the module: {Message}", ex.Message);

                            pluginStartedOn.Stop();
                            pluginInfo.RunTime = pluginStartedOn.Elapsed;
                            GC.Collect();
                            pluginInfo.CpuTimeFinish = GetCpuTime();
                            pluginInfo.TotalAllocationsFinish = GetTotalAllocatedBytes();
                            pluginInfo.AllocationDifferenceFinish = GetCurrentAllocatedBytes();
                            break; // stop processing plugins
                        }
                    }
                    catch (TransactionAbortedException)
                    {
                    }

                    pluginStartedOn.Stop();
                    pluginInfo.RunTime = pluginStartedOn.Elapsed;
                    GC.Collect();
                    pluginInfo.CpuTimeFinish = GetCpuTime();
                    pluginInfo.TotalAllocationsFinish = GetTotalAllocatedBytes();
                    pluginInfo.AllocationDifferenceFinish = GetCurrentAllocatedBytes();

                    sessionLogger.Log(LogSeverity.Information, false, null, null, "plugin finished in {Elapsed}", pluginStartedOn.Elapsed);
                    LogCounters(pluginInfo.Context.CounterCollection, sessionLogger);
                }

                sessionLogger.SetCurrentPlugin(null, null);

                LogCounters(sessionCounterCollection, sessionLogger);

                sessionLogger.Log(LogSeverity.Information, false, null, null, "--------------");
                sessionLogger.Log(LogSeverity.Information, false, null, null, "PLUGIN SUMMARY");
                sessionLogger.Log(LogSeverity.Information, false, null, null, "--------------");

                foreach (var pluginInfo in pluginInformation)
                {
                    if (pluginInfo.Context == null)
                        continue;

                    if (pluginInfo.Context.Result.Success)
                    {
                        sessionLogger.Log(LogSeverity.Information, false, null, null, "{Plugin} run-time is {Elapsed}, status is {Status}, CPU time: {CpuTime}, total allocations: {AllocatedMemory}, allocation difference: {MemoryDifference}",
                            pluginInfo.Plugin.Name, pluginInfo.RunTime, "success", pluginInfo.CpuTime, pluginInfo.TotalAllocations, pluginInfo.AllocationDifference);
                    }
                    else
                    {
                        sessionLogger.Log(LogSeverity.Information, false, null, null, "{Plugin} run-time is {Elapsed}, status is {Status}, requested to terminate execution: {TerminateHost}, CPU time: {CpuTime}, total allocations: {AllocatedMemory}, allocation difference: {MemoryDifference}",
                            pluginInfo.Plugin.Name, pluginInfo.RunTime, "failed", pluginInfo.Context.Result.TerminateHost, pluginInfo.CpuTime, pluginInfo.TotalAllocations, pluginInfo.AllocationDifference);
                    }
                }

                sessionLogger.SetCurrentPlugin(null, null);

                sessionLogger.Log(LogSeverity.Information, false, null, null, "---------------");
                sessionLogger.Log(LogSeverity.Information, false, null, null, "SESSION SUMMARY");
                sessionLogger.Log(LogSeverity.Information, false, null, null, "---------------");

                if (sessionWarningCount > 0)
                {
                    sessionLogger.Log(LogSeverity.Warning, false, null, null, "{Count} warnings/errors occured",
                        sessionWarningCount);
                }

                if (sessionExceptionCount > 0)
                {
                    sessionLogger.Log(LogSeverity.Warning, false, null, null, "{Count} exceptions raised",
                        sessionExceptionCount);
                }

                GC.Collect();

                sessionLogger.Log(LogSeverity.Information, false, null, null, "run-time is {Elapsed}, result is {Result}, CPU time: {CpuTime}, total allocations: {AllocatedMemory}, allocation difference: {MemoryDifference}",
                    sessionStartedOn.Elapsed, result, GetCpuTime().Subtract(sessionStartCpuTime), GetTotalAllocatedBytes() - sessionStartTotalAllocatedBytes, GetCurrentAllocatedBytes() - sessionStartCurrentAllocatedBytes);
            }
            catch (TransactionAbortedException)
            {
            }

            if (sessionLogger.DiagnosticsSender != null)
            {
                sessionLogger.DiagnosticsSender.Flush();
                sessionLogger.DiagnosticsSender.Dispose();
            }

            return result;
        }

        private static void LogCounters(StatCounterCollection counterCollection, ISessionLogger logger)
        {
            var counters = counterCollection.GetCounters();

            if (counters.Count == 0)
                return;

            if (logger.CurrentPlugin == null)
            {
                logger.Log(LogSeverity.Information, false, null, null, "----------------");
                logger.Log(LogSeverity.Information, false, null, null, "SESSION COUNTERS");
                logger.Log(LogSeverity.Information, false, null, null, "----------------");
            }
            else
            {
                logger.Log(LogSeverity.Information, false, null, null, "---------------");
                logger.Log(LogSeverity.Information, false, null, null, "PLUGIN COUNTERS");
                logger.Log(LogSeverity.Information, false, null, null, "---------------");
            }

            foreach (var counter in counters)
            {
                logger.Log(LogSeverity.Information, false, null, null, "{Counter} = {Value}",
                    counter.Name, counter.TypedValue);
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

        private class SessionPluginInfo
        {
            public IEtlPlugin Plugin { get; set; }
            public Module Module { get; set; }
            public TimeSpan RunTime { get; set; }
            public EtlContext Context { get; set; }

            public TimeSpan CpuTimeStart { get; set; }
            public long TotalAllocationsStart { get; set; }
            public long AllocationDifferenceStart { get; set; }

            public TimeSpan CpuTimeFinish { get; set; }
            public long TotalAllocationsFinish { get; set; }
            public long AllocationDifferenceFinish { get; set; }

            public TimeSpan CpuTime => CpuTimeFinish.Subtract(CpuTimeStart);
            public long TotalAllocations => TotalAllocationsFinish - TotalAllocationsStart;
            public long AllocationDifference => AllocationDifferenceFinish - AllocationDifferenceStart;
        }
    }
}