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

            var sessionId = "s" + Guid.NewGuid().ToString("N", CultureInfo.InvariantCulture).Substring(0, 6);

            var sessionStartedOn = Stopwatch.StartNew();
            var sessionWarningCount = 0;
            var sessionExceptionCount = 0;

            var mainContext = new ExecutionContext(sessionId, null, null, commandContext)
            {
                CustomCounterCollection = new StatCounterCollection(),
            };

            mainContext.Start();

            try
            {
                mainContext.Log(LogSeverity.Information, false, false, null, null, "session {SessionId} started", sessionId);

                var contextList = new List<ExecutionContext>();

                foreach (var module in modules)
                {
                    foreach (var plugin in module.EnabledPlugins)
                    {
                        var pluginContext = new ExecutionContext(sessionId, plugin, module, commandContext)
                        {
                            Topic = new Topic(null, new EtlContext(mainContext.CustomCounterCollection)
                            {
                                TransactionScopeTimeout = commandContext.HostConfiguration.TransactionScopeTimeout,
                            }),
                        };

                        contextList.Add(pluginContext);

                        pluginContext.Start();
                        pluginContext.ListenToEtlEvents();
                        pluginContext.Log(LogSeverity.Information, false, false, null, null, "plugin started");

                        try
                        {
                            try
                            {
                                plugin.Init(pluginContext.Topic, module.ModuleConfiguration);
                                plugin.BeforeExecute();
                                plugin.Execute();

                                sessionWarningCount += pluginContext.Context.Result.WarningCount;
                                sessionExceptionCount += pluginContext.Context.Result.Exceptions.Count;

                                if (pluginContext.Context.Result.TerminateHost)
                                {
                                    pluginContext.Log(LogSeverity.Error, false, false, null, null, "requested to terminate the execution of the module");

                                    result = ExecutionResult.PluginFailedAndExecutionTerminated;
                                    pluginContext.Finish();
                                    pluginContext.Close();
                                    break; // stop processing plugins
                                }

                                if (!pluginContext.Context.Result.Success)
                                {
                                    result = ExecutionResult.PluginFailed;
                                }
                            }
                            catch (Exception ex)
                            {
                                result = ExecutionResult.PluginFailedAndExecutionTerminated;
                                pluginContext.Finish();

                                pluginContext.Log(LogSeverity.Error, false, false, null, null, "unhandled error during plugin execution after {Elapsed}, message: {Message}", pluginContext.RunTime, ex.Message);
                                pluginContext.Log(LogSeverity.Error, true, false, null, null, "requested to terminate the execution of the module: {Message}", ex.Message);

                                pluginContext.Close();
                                break; // stop processing plugins
                            }
                        }
                        catch (TransactionAbortedException)
                        {
                        }

                        pluginContext.Finish();
                        pluginContext.Log(LogSeverity.Information, false, false, null, null, "plugin finished in {Elapsed}", pluginContext.RunTime);
                        pluginContext.LogCounters();

                        pluginContext.Close();
                    }
                }

                mainContext.Finish();
                mainContext.LogCounters();

                mainContext.Log(LogSeverity.Information, false, false, null, null, "--------------");
                mainContext.Log(LogSeverity.Information, false, false, null, null, "PLUGIN SUMMARY");
                mainContext.Log(LogSeverity.Information, false, false, null, null, "--------------");

                foreach (var pluginContext in contextList)
                {
                    if (pluginContext.Context == null)
                        continue;

                    if (pluginContext.Context.Result.Success)
                    {
                        mainContext.Log(LogSeverity.Information, false, false, null, null, "{Plugin} run-time is {Elapsed}, status is {Status}, CPU time: {CpuTime}, total allocations: {AllocatedMemory}, allocation difference: {MemoryDifference}",
                            pluginContext.PluginName, pluginContext.RunTime, "success", pluginContext.CpuTime, pluginContext.TotalAllocations, pluginContext.AllocationDifference);
                    }
                    else
                    {
                        mainContext.Log(LogSeverity.Information, false, false, null, null, "{Plugin} run-time is {Elapsed}, status is {Status}, requested to terminate execution: {TerminateHost}, CPU time: {CpuTime}, total allocations: {AllocatedMemory}, allocation difference: {MemoryDifference}",
                            pluginContext.PluginName, pluginContext.RunTime, "failed", pluginContext.Context.Result.TerminateHost, pluginContext.CpuTime, pluginContext.TotalAllocations, pluginContext.AllocationDifference);
                    }
                }

                mainContext.Log(LogSeverity.Information, false, false, null, null, "---------------");
                mainContext.Log(LogSeverity.Information, false, false, null, null, "SESSION SUMMARY");
                mainContext.Log(LogSeverity.Information, false, false, null, null, "---------------");

                if (sessionWarningCount > 0)
                {
                    mainContext.Log(LogSeverity.Warning, false, false, null, null, "{Count} warnings/errors occured",
                        sessionWarningCount);
                }

                if (sessionExceptionCount > 0)
                {
                    mainContext.Log(LogSeverity.Warning, false, false, null, null, "{Count} exceptions raised",
                        sessionExceptionCount);
                }
            }
            catch (TransactionAbortedException)
            {
            }

            mainContext.Finish();

            mainContext.Log(LogSeverity.Information, false, false, null, null, "run-time is {Elapsed}, result is {Result}, CPU time: {CpuTime}, total allocations: {AllocatedMemory}, allocation difference: {MemoryDifference}",
                sessionStartedOn.Elapsed, result, mainContext.CpuTime, mainContext.TotalAllocations, mainContext.AllocationDifference);

            mainContext.Close();

            return result;
        }
    }
}