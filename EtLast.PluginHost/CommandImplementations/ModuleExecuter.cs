namespace FizzCode.EtLast.PluginHost
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Transactions;
    using FizzCode.EtLast;

    internal static class ModuleExecuter
    {
        public static ExecutionResult Execute(CommandContext commandContext, Module module)
        {
            var result = ExecutionResult.Success;

            IModuleLogger logger = new SerilogModuleLogger()
            {
                Logger = commandContext.Logger,
                OpsLogger = commandContext.OpsLogger,
                ModuleConfiguration = module.ModuleConfiguration,
                DiagnosticsSender = commandContext.HostConfiguration.DiagnosticsUri != null
                    ? new HttpDiagnosticsSender(commandContext.HostConfiguration.DiagnosticsUri)
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

                logger.Log(LogSeverity.Information, false, null, null, null, "module started");

                foreach (var plugin in module.EnabledPlugins)
                {
                    var pluginStartedOn = Stopwatch.StartNew();
                    var friendlyPluginName = TypeHelpers.GetFriendlyTypeName(plugin.GetType());

                    logger.Log(LogSeverity.Information, false, plugin, null, null, "plugin started");

                    GC.Collect();
                    var pluginStartCpuTime = GetCpuTime();
                    var pluginStartTotalAllocatedBytes = GetTotalAllocatedBytes();
                    var pluginStartCurrentAllocatedBytes = GetCurrentAllocatedBytes();

                    try
                    {
                        try
                        {
                            var pluginContext = GetEtlContextForPlugin(commandContext, logger, moduleCounterCollection, plugin);
                            pluginResults.Add(pluginContext.Result);

                            plugin.Init(pluginContext, module.ModuleConfiguration);
                            plugin.Execute();

                            LogCounters(pluginContext.CounterCollection, logger, plugin);

                            moduleWarningCount += pluginContext.Result.WarningCount;
                            moduleExceptionCount += pluginContext.Result.Exceptions.Count;

                            if (pluginContext.Result.TerminateHost)
                            {
                                logger.Log(LogSeverity.Error, false, plugin, null, null, "requested to terminate the execution of the module");

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

                            logger.Log(LogSeverity.Error, false, plugin, null, null, "unhandled error during plugin execution after {Elapsed}", pluginStartedOn.Elapsed);
                            logger.Log(LogSeverity.Error, true, plugin, null, null, "requested to terminate the execution of the module: {Message}", ex.Message);

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

                    logger.Log(LogSeverity.Information, false, plugin, null, null, "finished in {Elapsed}", pluginStartedOn.Elapsed);
                }

                LogCounters(moduleCounterCollection, logger, null);

                for (var i = 0; i < Math.Min(module.EnabledPlugins.Count, pluginResults.Count); i++)
                {
                    var plugin = module.EnabledPlugins[i];
                    var pluginResult = pluginResults[i];
                    if (pluginResult.Success)
                    {
                        logger.Log(LogSeverity.Information, false, plugin, null, null, "run-time is {Elapsed}, status is {Status}, CPU time: {CpuTime}, total allocations: {AllocatedMemory}, allocation difference: {MemoryDifference}",
                            runTimes[i], "success", cpuTimes[i], pluginTotalAllocations[i], pluginAllocationDifferences[i]);
                    }
                    else
                    {
                        logger.Log(LogSeverity.Information, false, plugin, null, null, "run-time is {Elapsed}, status is {Status}, requested to terminate execution: {TerminateHost}, CPU time: {CpuTime}, total allocations: {AllocatedMemory}, allocation difference: {MemoryDifference}",
                            runTimes[i], "failed", pluginResult.TerminateHost, cpuTimes[i], pluginTotalAllocations[i], pluginAllocationDifferences[i]);
                    }
                }

                if (moduleWarningCount > 0)
                {
                    logger.Log(LogSeverity.Warning, false, null, null, null, "{Count} warnings/errors occured during module execution",
                        moduleWarningCount);
                }

                if (moduleExceptionCount > 0)
                {
                    logger.Log(LogSeverity.Warning, false, null, null, null, "{Count} exceptions raised during module execution",
                        moduleExceptionCount);
                }

                GC.Collect();

                logger.Log(LogSeverity.Information, false, null, null, null, "run-time is {Elapsed}, result is {Result}, CPU time: {CpuTime}, total allocations: {AllocatedMemory}, allocation difference: {MemoryDifference}",
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

        private static EtlContext GetEtlContextForPlugin(CommandContext commandContext, IModuleLogger logger, StatCounterCollection moduleCounterCollection, IEtlPlugin plugin)
        {
            return new EtlContext(moduleCounterCollection)
            {
                TransactionScopeTimeout = commandContext.HostConfiguration.TransactionScopeTimeout,
                OnException = (sender, args) => logger.LogException(plugin, args),
                OnLog = (severity, forOps, process, operation, text, args) => logger.Log(severity, forOps, plugin, process, operation, text, args),
                OnCustomLog = (forOps, fileName, process, text, args) => logger.LogCustom(forOps, plugin, fileName, process, text, args),
                OnRowCreated = (row, creatorProcess) => logger.LifecycleRowCreated(plugin, row, creatorProcess),
                OnRowOwnerChanged = (row, previousProcess, currentProcess) => logger.LifecycleRowOwnerChanged(plugin, row, previousProcess, currentProcess),
                OnRowValueChanged = (row, column, previousValue, newValue, process, operation) => logger.LifecycleRowValueChanged(plugin, row, column, previousValue, newValue, process, operation),
                OnRowStored = (row, location) => logger.LifecycleRowStored(plugin, row, location),
            };
        }

        private static void LogCounters(StatCounterCollection counterCollection, IModuleLogger logger, IEtlPlugin plugin)
        {
            var counters = counterCollection.GetCounters()
                .Where(counter => !counter.IsDebug)
                .ToList();

            if (counters.Count == 0)
                return;

            foreach (var counter in counters)
            {
                logger.Log(counter.IsDebug ? LogSeverity.Debug : LogSeverity.Information, false, plugin, null, null, "counter {Counter} = {Value}",
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