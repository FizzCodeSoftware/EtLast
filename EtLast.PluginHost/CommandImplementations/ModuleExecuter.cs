namespace FizzCode.EtLast.PluginHost
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;
    using System.Linq;
    using System.Transactions;
    using FizzCode.EtLast;

    internal static class ModuleExecuter
    {
        public static ExecutionResult Execute(CommandContext commandContext, Module module)
        {
            var result = ExecutionResult.Success;

            var sessionId = Guid.NewGuid().ToString("N", CultureInfo.InvariantCulture).Substring(0, 6);

            IModuleLogger logger = new SerilogModuleLogger()
            {
                Logger = commandContext.Logger,
                OpsLogger = commandContext.OpsLogger,
                ModuleConfiguration = module.ModuleConfiguration,
                DiagnosticsSender = commandContext.HostConfiguration.DiagnosticsUri != null
                    ? new HttpDiagnosticsSender(sessionId, commandContext.HostConfiguration.DiagnosticsUri)
                    : null,
            };

            GC.Collect();

            var moduleStartedOn = Stopwatch.StartNew();
            var moduleStartCpuTime = GetCpuTime();
            var moduleStartTotalAllocatedBytes = GetTotalAllocatedBytes();
            var moduleStartCurrentAllocatedBytes = GetCurrentAllocatedBytes();
            var moduleCounterCollection = new StatCounterCollection();
            var moduleWarningCount = 0;
            var moduleExceptionCount = 0;

            try
            {
                var runTimes = new List<TimeSpan>();
                var pluginResults = new List<EtlContextResult>();
                var cpuTimes = new List<TimeSpan>();
                var pluginTotalAllocations = new List<long>();
                var pluginAllocationDifferences = new List<long>();

                logger.Log(LogSeverity.Information, false, null, null, "module started, session: {SessionId}", sessionId);

                foreach (var plugin in module.EnabledPlugins)
                {
                    var pluginStartedOn = Stopwatch.StartNew();
                    var friendlyPluginName = TypeHelpers.GetFriendlyTypeName(plugin.GetType());

                    var pluginContext = new EtlContext(moduleCounterCollection)
                    {
                        TransactionScopeTimeout = commandContext.HostConfiguration.TransactionScopeTimeout,
                    };

                    pluginResults.Add(pluginContext.Result);

                    logger.SetCurrentPlugin(plugin);
                    logger.SetupContextEvents(pluginContext);

                    logger.Log(LogSeverity.Information, false, null, null, "plugin started");

                    GC.Collect();
                    var pluginStartCpuTime = GetCpuTime();
                    var pluginStartTotalAllocatedBytes = GetTotalAllocatedBytes();
                    var pluginStartCurrentAllocatedBytes = GetCurrentAllocatedBytes();

                    try
                    {
                        try
                        {
                            plugin.Init(pluginContext, module.ModuleConfiguration);
                            plugin.Execute();

                            moduleWarningCount += pluginContext.Result.WarningCount;
                            moduleExceptionCount += pluginContext.Result.Exceptions.Count;

                            if (pluginContext.Result.TerminateHost)
                            {
                                logger.Log(LogSeverity.Error, false, null, null, "requested to terminate the execution of the module");

                                result = ExecutionResult.PluginFailedAndExecutionTerminated;

                                pluginStartedOn.Stop();
                                runTimes.Add(pluginStartedOn.Elapsed);
                                GC.Collect();
                                cpuTimes.Add(GetCpuTime().Subtract(pluginStartCpuTime));
                                pluginTotalAllocations.Add(GetTotalAllocatedBytes() - pluginStartTotalAllocatedBytes);
                                pluginAllocationDifferences.Add(GetCurrentAllocatedBytes() - pluginStartCurrentAllocatedBytes);

                                break; // stop processing plugins
                            }

                            if (!pluginContext.Result.Success)
                            {
                                result = ExecutionResult.PluginFailed;
                            }
                        }
                        catch (Exception ex)
                        {
                            result = ExecutionResult.PluginFailedAndExecutionTerminated;

                            logger.Log(LogSeverity.Error, false, null, null, "unhandled error during plugin execution after {Elapsed}", pluginStartedOn.Elapsed);
                            logger.Log(LogSeverity.Error, true, null, null, "requested to terminate the execution of the module: {Message}", ex.Message);

                            pluginStartedOn.Stop();
                            runTimes.Add(pluginStartedOn.Elapsed);
                            GC.Collect();
                            cpuTimes.Add(GetCpuTime().Subtract(pluginStartCpuTime));
                            pluginTotalAllocations.Add(GetTotalAllocatedBytes() - pluginStartTotalAllocatedBytes);
                            pluginAllocationDifferences.Add(GetCurrentAllocatedBytes() - pluginStartCurrentAllocatedBytes);

                            break; // stop processing plugins
                        }
                    }
                    catch (TransactionAbortedException)
                    {
                    }

                    pluginStartedOn.Stop();
                    runTimes.Add(pluginStartedOn.Elapsed);
                    GC.Collect();
                    cpuTimes.Add(GetCpuTime().Subtract(pluginStartCpuTime));
                    pluginTotalAllocations.Add(GetTotalAllocatedBytes() - pluginStartTotalAllocatedBytes);
                    pluginAllocationDifferences.Add(GetCurrentAllocatedBytes() - pluginStartCurrentAllocatedBytes);

                    logger.Log(LogSeverity.Information, false, null, null, "plugin finished in {Elapsed}", pluginStartedOn.Elapsed);
                    LogCounters(pluginContext.CounterCollection, logger);
                }

                logger.SetCurrentPlugin(null);

                LogCounters(moduleCounterCollection, logger);

                logger.Log(LogSeverity.Information, false, null, null, "---------------------");
                logger.Log(LogSeverity.Information, false, null, null, "PLUGIN RESULT SUMMARY");
                logger.Log(LogSeverity.Information, false, null, null, "---------------------");

                for (var i = 0; i < Math.Min(module.EnabledPlugins.Count, pluginResults.Count); i++)
                {
                    var plugin = module.EnabledPlugins[i];
                    var pluginResult = pluginResults[i];

                    if (pluginResult.Success)
                    {
                        logger.Log(LogSeverity.Information, false, null, null, "{Plugin} run-time is {Elapsed}, status is {Status}, CPU time: {CpuTime}, total allocations: {AllocatedMemory}, allocation difference: {MemoryDifference}",
                            plugin.Name, runTimes[i], "success", cpuTimes[i], pluginTotalAllocations[i], pluginAllocationDifferences[i]);
                    }
                    else
                    {
                        logger.Log(LogSeverity.Information, false, null, null, "{Plugin} run-time is {Elapsed}, status is {Status}, requested to terminate execution: {TerminateHost}, CPU time: {CpuTime}, total allocations: {AllocatedMemory}, allocation difference: {MemoryDifference}",
                            plugin.Name, runTimes[i], "failed", pluginResult.TerminateHost, cpuTimes[i], pluginTotalAllocations[i], pluginAllocationDifferences[i]);
                    }
                }

                logger.SetCurrentPlugin(null);

                logger.Log(LogSeverity.Information, false, null, null, "---------------------");
                logger.Log(LogSeverity.Information, false, null, null, "MODULE RESULT SUMMARY");
                logger.Log(LogSeverity.Information, false, null, null, "---------------------");

                if (moduleWarningCount > 0)
                {
                    logger.Log(LogSeverity.Warning, false, null, null, "{Count} warnings/errors occured",
                        moduleWarningCount);
                }

                if (moduleExceptionCount > 0)
                {
                    logger.Log(LogSeverity.Warning, false, null, null, "{Count} exceptions raised",
                        moduleExceptionCount);
                }

                GC.Collect();

                logger.Log(LogSeverity.Information, false, null, null, "run-time is {Elapsed}, result is {Result}, CPU time: {CpuTime}, total allocations: {AllocatedMemory}, allocation difference: {MemoryDifference}",
                    moduleStartedOn.Elapsed, result, GetCpuTime().Subtract(moduleStartCpuTime), GetTotalAllocatedBytes() - moduleStartTotalAllocatedBytes, GetCurrentAllocatedBytes() - moduleStartCurrentAllocatedBytes);
            }
            catch (TransactionAbortedException)
            {
            }

            if (logger.DiagnosticsSender != null)
            {
                logger.DiagnosticsSender.Flush();
                logger.DiagnosticsSender.Dispose();
            }

            return result;
        }

        private static void LogCounters(StatCounterCollection counterCollection, IModuleLogger logger)
        {
            var counters = counterCollection.GetCounters()
                .Where(counter => !counter.IsDebug)
                .ToList();

            if (counters.Count == 0)
                return;

            if (logger.CurrentPlugin == null)
            {
                logger.Log(LogSeverity.Information, false, null, null, "---------------");
                logger.Log(LogSeverity.Information, false, null, null, "MODULE COUNTERS");
                logger.Log(LogSeverity.Information, false, null, null, "---------------");
            }
            else
            {
                logger.Log(LogSeverity.Information, false, null, null, "---------------");
                logger.Log(LogSeverity.Information, false, null, null, "PLUGIN COUNTERS");
                logger.Log(LogSeverity.Information, false, null, null, "---------------");
            }

            foreach (var counter in counters)
            {
                logger.Log(counter.IsDebug ? LogSeverity.Debug : LogSeverity.Information, false, null, null, "{Counter} = {Value}",
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
    }
}