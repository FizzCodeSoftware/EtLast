namespace FizzCode.EtLast.PluginHost
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;
    using System.Linq;
    using System.Text;
    using System.Transactions;
    using Serilog;

    internal static class ModuleExecuter
    {
        public static ExecutionResult Execute(CommandContext commandContext, Module module)
        {
            var result = ExecutionResult.Success;

            GC.Collect();
            var cpuTime = GetCpuTime();
            var lifetimeMemory = GetLifetimeMemory();
            var currentMemory = GetCurrentMemory();

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

                foreach (var plugin in module.EnabledPlugins)
                {
                    var pluginStartedOn = Stopwatch.StartNew();
                    var friendlyPluginName = TypeHelpers.GetFriendlyTypeName(plugin.GetType());
                    commandContext.Logger.Information("[{Module}/{Plugin}] plugin started", module.ModuleConfiguration.ModuleName, friendlyPluginName);

                    GC.Collect();
                    var pluginCpuTime = GetCpuTime();
                    var pluginLifetimeMemory = GetLifetimeMemory();
                    var pluginCurrentMemory = GetCurrentMemory();

                    try
                    {
                        try
                        {
                            plugin.Init(commandContext.Logger, commandContext.OpsLogger, module.ModuleConfiguration, commandContext.HostConfiguration.TransactionScopeTimeout, moduleCounterCollection);
                            pluginResults.Add(plugin.Context.Result);

                            plugin.BeforeExecute();
                            plugin.Execute();
                            plugin.AfterExecute();

                            moduleWarningCount += plugin.Context.Result.WarningCount;
                            moduleExceptionCount += plugin.Context.Result.Exceptions.Count;

                            if (plugin.Context.Result.TerminateHost)
                            {
                                commandContext.Logger.Error("[{Module}/{Plugin}] requested to terminate the execution of the module", module.ModuleConfiguration.ModuleName, friendlyPluginName);
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
                            commandContext.Logger.Error(ex, "[{Module}/{Plugin}] unhandled error during plugin execution after {Elapsed}", module.ModuleConfiguration.ModuleName, pluginStartedOn.Elapsed, friendlyPluginName);
                            commandContext.OpsLogger.Error("[{Module}/{Plugin}] unhandled error during plugin execution after {Elapsed}: {Message}", module.ModuleConfiguration.ModuleName, pluginStartedOn.Elapsed, friendlyPluginName, ex.Message);

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

                    commandContext.Logger.Information(
                        "[{Module}/{Plugin}] finished in {Elapsed}",
                        module.ModuleConfiguration.ModuleName,
                        friendlyPluginName,
                        pluginStartedOn.Elapsed);
                }

                LogCounters(module, moduleCounterCollection, commandContext.Logger);

                for (var i = 0; i < Math.Min(module.EnabledPlugins.Count, pluginResults.Count); i++)
                {
                    var plugin = module.EnabledPlugins[i];
                    var pluginResult = pluginResults[i];
                    if (pluginResult.Success)
                    {
                        commandContext.Logger.Information("[{Module}/{Plugin}] run-time is {Elapsed}, status is {Status}, CPU time: {CpuTime}, allocated memory: {AllocatedMemory}, survived memory: {SurvivedMemory}",
                            module.ModuleConfiguration.ModuleName,
                            TypeHelpers.GetFriendlyTypeName(plugin.GetType()),
                            runTimes[i],
                            "success",
                            cpuTimes[i],
                            lifetimeMemories[i],
                            currentMemories[i]);
                    }
                    else
                    {
                        commandContext.Logger.Information("[{Module}/{Plugin}] run-time is {Elapsed}, status is {Status}, requested to terminate execution: {TerminateHost}, CPU time: {CpuTime}, allocated memory: {AllocatedMemory}, survived memory: {SurvivedMemory}",
                            module.ModuleConfiguration.ModuleName,
                            TypeHelpers.GetFriendlyTypeName(plugin.GetType()),
                            runTimes[i],
                            "failed",
                            pluginResult.TerminateHost,
                            cpuTimes[i],
                            lifetimeMemories[i],
                            currentMemories[i]);
                    }
                }

                if (moduleWarningCount > 0)
                    commandContext.Logger.Warning("[{Module}] {Count} warnings/errors occured during module execution", module.ModuleConfiguration.ModuleName, moduleWarningCount);

                if (moduleExceptionCount > 0)
                    commandContext.Logger.Warning("[{Module}] {Count} exceptions raised during module execution", module.ModuleConfiguration.ModuleName, moduleExceptionCount);

                GC.Collect();
                commandContext.Logger.Information("[{Module}] run-time is {Elapsed}, result is {Result}, CPU time: {CpuTime}, allocated memory: {AllocatedMemory}, survived memory: {SurvivedMemory}",
                    module.ModuleConfiguration.ModuleName,
                    moduleStartedOn.Elapsed,
                    result,
                    GetCpuTime().Subtract(cpuTime),
                    GetLifetimeMemory() - lifetimeMemory,
                    GetCurrentMemory() - currentMemory);
            }
            catch (TransactionAbortedException)
            {
            }

            return result;
        }

        private static void LogCounters(Module module, StatCounterCollection counterCollection, ILogger logger)
        {
            var counters = counterCollection.GetCounters()
                .Where(counter => !counter.IsDebug)
                .ToList();

            if (counters.Count == 0)
                return;

            var sb = new StringBuilder();
            var parameters = new List<object>();

            foreach (var counter in counters)
            {
                sb.Append("[{Module}] counter {Counter} = {Value}");
                parameters.Add(module.ModuleConfiguration.ModuleName);
                parameters.Add(counter.Name);
                parameters.Add(counter.Value.TypedValue);

                if (counter.SubValues != null)
                {
                    var idx = 0;
                    foreach (var kvp in counter.SubValues)
                    {
                        sb
                            .Append(", {Sub")
                            .Append(idx.ToString("D", CultureInfo.InvariantCulture))
                            .Append("} = {SubValue")
                            .Append(idx.ToString("D", CultureInfo.InvariantCulture))
                            .Append('}');

                        parameters.Add(kvp.Key);
                        parameters.Add(kvp.Value.TypedValue);
                        idx++;
                    }
                }

                logger.Write(counter.IsDebug ? Serilog.Events.LogEventLevel.Debug : Serilog.Events.LogEventLevel.Information, sb.ToString(), parameters.ToArray());
                sb.Clear();
                parameters.Clear();
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