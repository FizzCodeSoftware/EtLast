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
                mainContext.Log(LogSeverity.Information, false, false, null, "session {SessionId} started", sessionId);

                var contextList = new List<ExecutionContext>();

                foreach (var module in modules)
                {
                    foreach (var plugin in module.EnabledPlugins)
                    {
                        var pluginContext = new ExecutionContext(sessionId, plugin, module, commandContext)
                        {
                            Context = new EtlContext(mainContext.CustomCounterCollection)
                            {
                                TransactionScopeTimeout = commandContext.HostConfiguration.TransactionScopeTimeout,
                            },
                        };

                        contextList.Add(pluginContext);

                        pluginContext.Start();
                        pluginContext.ListenToEtlEvents();
                        pluginContext.Log(LogSeverity.Information, false, false, null, "plugin started");

                        try
                        {
                            try
                            {
                                plugin.Init(pluginContext.Context, module.ModuleConfiguration);
                                plugin.BeforeExecute();
                                plugin.Execute();

                                sessionWarningCount += pluginContext.Context.Result.WarningCount;
                                sessionExceptionCount += pluginContext.Context.Result.Exceptions.Count;

                                if (pluginContext.Context.Result.TerminateHost)
                                {
                                    pluginContext.Log(LogSeverity.Error, false, false, null, "requested to terminate the execution of the module");

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

                                pluginContext.Log(LogSeverity.Error, false, false, null, "unhandled error during plugin execution after {Elapsed}, message: ", pluginContext.RunTime, ex.Message);
                                pluginContext.Log(LogSeverity.Error, true, false, null, "requested to terminate the execution of the module: {Message}", ex.Message);

                                pluginContext.Close();
                                break; // stop processing plugins
                            }
                        }
                        catch (TransactionAbortedException)
                        {
                        }

                        pluginContext.Finish();
                        pluginContext.Log(LogSeverity.Information, false, false, null, "plugin finished in {Elapsed}", pluginContext.RunTime);
                        pluginContext.LogCounters();

                        pluginContext.Close();
                    }
                }

                mainContext.Finish();
                mainContext.LogCounters();

                mainContext.Log(LogSeverity.Information, false, false, null, "--------------");
                mainContext.Log(LogSeverity.Information, false, false, null, "PLUGIN SUMMARY");
                mainContext.Log(LogSeverity.Information, false, false, null, "--------------");

                foreach (var pluginContext in contextList)
                {
                    if (pluginContext.Context == null)
                        continue;

                    if (pluginContext.Context.Result.Success)
                    {
                        mainContext.Log(LogSeverity.Information, false, false, null, "{Plugin} run-time is {Elapsed}, status is {Status}, CPU time: {CpuTime}, total allocations: {AllocatedMemory}, allocation difference: {MemoryDifference}",
                            pluginContext.PluginName, pluginContext.RunTime, "success", pluginContext.CpuTime, pluginContext.TotalAllocations, pluginContext.AllocationDifference);
                    }
                    else
                    {
                        mainContext.Log(LogSeverity.Information, false, false, null, "{Plugin} run-time is {Elapsed}, status is {Status}, requested to terminate execution: {TerminateHost}, CPU time: {CpuTime}, total allocations: {AllocatedMemory}, allocation difference: {MemoryDifference}",
                            pluginContext.PluginName, pluginContext.RunTime, "failed", pluginContext.Context.Result.TerminateHost, pluginContext.CpuTime, pluginContext.TotalAllocations, pluginContext.AllocationDifference);
                    }
                }

                mainContext.Log(LogSeverity.Information, false, false, null, "---------------");
                mainContext.Log(LogSeverity.Information, false, false, null, "SESSION SUMMARY");
                mainContext.Log(LogSeverity.Information, false, false, null, "---------------");

                if (sessionWarningCount > 0)
                {
                    mainContext.Log(LogSeverity.Warning, false, false, null, "{Count} warnings/errors occured",
                        sessionWarningCount);
                }

                if (sessionExceptionCount > 0)
                {
                    mainContext.Log(LogSeverity.Warning, false, false, null, "{Count} exceptions raised",
                        sessionExceptionCount);
                }
            }
            catch (TransactionAbortedException)
            {
            }

            mainContext.Finish();

            mainContext.Log(LogSeverity.Information, false, false, null, "run-time is {Elapsed}, result is {Result}, CPU time: {CpuTime}, total allocations: {AllocatedMemory}, allocation difference: {MemoryDifference}",
                sessionStartedOn.Elapsed, result, mainContext.CpuTime, mainContext.TotalAllocations, mainContext.AllocationDifference);

            mainContext.Close();

            return result;
        }
    }
}