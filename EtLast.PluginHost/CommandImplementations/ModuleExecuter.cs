namespace FizzCode.EtLast.PluginHost
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Transactions;

    internal static class ModuleExecuter
    {
        public static ExecutionResult Execute(CommandContext commandContext, Module module)
        {
            var result = ExecutionResult.Success;

            GC.Collect();
            var cpuTime = GetCpuTime();
            var lifetimeMemory = GetLifetimeMemory();
            var currentMemory = GetCurrentMemory();

            using var logger = new ModuleSerilogLogger()
            {
                Logger = commandContext.Logger,
                OpsLogger = commandContext.OpsLogger,
                ModuleConfiguration = module.ModuleConfiguration,
                DiagnosticsUri = commandContext.HostConfiguration.DiagnosticsUri,
            };

            try
            {
                var moduleStartedOn = Stopwatch.StartNew();
                var moduleCounterCollection = new StatCounterCollection();
                var runTimes = new List<TimeSpan>();
                var pluginResults = new List<EtlContextResult>();
                var cpuTimes = new List<TimeSpan>();
                var lifetimeMemories = new List<long>();
                var currentMemories = new List<long>();

                var moduleWarningCount = 0;
                var moduleExceptionCount = 0;

                logger.Log(LogSeverity.Information, false, null, null, null, "module started");

                foreach (var plugin in module.EnabledPlugins)
                {
                    var pluginStartedOn = Stopwatch.StartNew();
                    var friendlyPluginName = TypeHelpers.GetFriendlyTypeName(plugin.GetType());

                    logger.Log(LogSeverity.Information, false, plugin, null, null, "plugin started");

                    GC.Collect();
                    var pluginCpuTime = GetCpuTime();
                    var pluginLifetimeMemory = GetLifetimeMemory();
                    var pluginCurrentMemory = GetCurrentMemory();

                    try
                    {
                        try
                        {
                            plugin.Init(logger, module.ModuleConfiguration, commandContext.HostConfiguration.TransactionScopeTimeout, moduleCounterCollection);
                            pluginResults.Add(plugin.Context.Result);

                            plugin.BeforeExecute();
                            plugin.Execute();
                            plugin.AfterExecute();

                            moduleWarningCount += plugin.Context.Result.WarningCount;
                            moduleExceptionCount += plugin.Context.Result.Exceptions.Count;

                            if (plugin.Context.Result.TerminateHost)
                            {
                                logger.Log(LogSeverity.Error, false, plugin, null, null, "requested to terminate the execution of the module");

                                result = ExecutionResult.PluginFailedAndExecutionTerminated;

                                pluginStartedOn.Stop();
                                runTimes.Add(pluginStartedOn.Elapsed);
                                GC.Collect();
                                cpuTimes.Add(GetCpuTime().Subtract(pluginCpuTime));
                                lifetimeMemories.Add(GetLifetimeMemory() - pluginLifetimeMemory);
                                currentMemories.Add(GetCurrentMemory() - pluginCurrentMemory);

                                break; // stop processing plugins
                            }

                            if (!plugin.Context.Result.Success)
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
                            cpuTimes.Add(GetCpuTime().Subtract(pluginCpuTime));
                            lifetimeMemories.Add(GetLifetimeMemory() - pluginLifetimeMemory);
                            currentMemories.Add(GetCurrentMemory() - pluginCurrentMemory);

                            break; // stop processing plugins
                        }
                    }
                    catch (TransactionAbortedException)
                    {
                    }

                    pluginStartedOn.Stop();
                    runTimes.Add(pluginStartedOn.Elapsed);
                    GC.Collect();
                    cpuTimes.Add(GetCpuTime().Subtract(pluginCpuTime));
                    lifetimeMemories.Add(GetLifetimeMemory() - pluginLifetimeMemory);
                    currentMemories.Add(GetCurrentMemory() - pluginCurrentMemory);

                    logger.Log(LogSeverity.Information, false, plugin, null, null, "finished in {Elapsed}", pluginStartedOn.Elapsed);
                }

                LogModuleCounters(moduleCounterCollection, logger);

                for (var i = 0; i < Math.Min(module.EnabledPlugins.Count, pluginResults.Count); i++)
                {
                    var plugin = module.EnabledPlugins[i];
                    var pluginResult = pluginResults[i];
                    if (pluginResult.Success)
                    {
                        logger.Log(LogSeverity.Information, false, plugin, null, null, "run-time is {Elapsed}, status is {Status}, CPU time: {CpuTime}, allocated memory: {AllocatedMemory}, survived memory: {SurvivedMemory}",
                            runTimes[i], "success", cpuTimes[i], lifetimeMemories[i], currentMemories[i]);
                    }
                    else
                    {
                        logger.Log(LogSeverity.Information, false, plugin, null, null, "run-time is {Elapsed}, status is {Status}, requested to terminate execution: {TerminateHost}, CPU time: {CpuTime}, allocated memory: {AllocatedMemory}, survived memory: {SurvivedMemory}",
                            runTimes[i], "failed", pluginResult.TerminateHost, cpuTimes[i], lifetimeMemories[i], currentMemories[i]);
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

                logger.Log(LogSeverity.Information, false, null, null, null, "run-time is {Elapsed}, result is {Result}, CPU time: {CpuTime}, allocated memory: {AllocatedMemory}, survived memory: {SurvivedMemory}",
                    moduleStartedOn.Elapsed, result, GetCpuTime().Subtract(cpuTime), GetLifetimeMemory() - lifetimeMemory, GetCurrentMemory() - currentMemory);
            }
            catch (TransactionAbortedException)
            {
            }

            return result;
        }

        private static void LogModuleCounters(StatCounterCollection counterCollection, IEtlPluginLogger logger)
        {
            var counters = counterCollection.GetCounters()
                .Where(counter => !counter.IsDebug)
                .ToList();

            if (counters.Count == 0)
                return;

            foreach (var counter in counters)
            {
                logger.Log(counter.IsDebug ? LogSeverity.Debug : LogSeverity.Information, false, null, null, null, "counter {Counter} = {Value}",
                    counter.Name, counter.TypedValue);
            }
        }

        private static TimeSpan GetCpuTime()
        {
            return AppDomain.CurrentDomain.MonitoringTotalProcessorTime;
        }

        private static long GetCurrentMemory()
        {
            return GC.GetTotalMemory(false);
        }

        private static long GetLifetimeMemory()
        {
            return GC.GetTotalAllocatedBytes(true);
        }
    }
}