namespace FizzCode.EtLast.PluginHost
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;
    using System.Linq;
    using System.Transactions;
    using FizzCode.EtLast;
    using FizzCode.EtLast.PluginHost.SerilogSink;

    internal static class ModuleExecuter
    {
        public static ExecutionResult Execute(CommandContext commandContext, params Module[] modules)
        {
            var result = ExecutionResult.Success;

            var sessionId = "s" + Guid.NewGuid().ToString("N", CultureInfo.InvariantCulture).Substring(0, 6);

            var sessionStartedOn = Stopwatch.StartNew();
            var sessionWarningCount = 0;
            var sessionExceptionCount = 0;

            var sessionContext = new ExecutionContext(null, null, sessionId, null, null, commandContext);
            result = sessionContext.Start();
            if (result != ExecutionResult.Success)
                return result;

            try
            {
                sessionContext.OnLog(LogSeverity.Information, false, null, null, "session {SessionId} started", sessionId);

                var contextList = new List<ExecutionContext>();

                foreach (var module in modules)
                {
                    foreach (var plugin in module.EnabledPlugins)
                    {
                        var topic = new Topic(null, new EtlContext()
                        {
                            TransactionScopeTimeout = commandContext.HostConfiguration.TransactionScopeTimeout,
                        });

                        var pluginContext = new ExecutionContext(sessionContext, topic, sessionId, plugin, module, commandContext);

                        contextList.Add(pluginContext);

                        result = pluginContext.Start();
                        if (result != ExecutionResult.Success)
                            return result;

                        pluginContext.Topic.Context.Listeners.Add(pluginContext);

                        pluginContext.OnLog(LogSeverity.Information, false, null, null, "plugin started");

                        try
                        {
                            try
                            {
                                plugin.Init(pluginContext.Topic, module.ModuleConfiguration);
                                plugin.BeforeExecute();
                                plugin.Execute();

                                sessionWarningCount += pluginContext.Topic.Context.Result.WarningCount;
                                sessionExceptionCount += pluginContext.Topic.Context.Result.Exceptions.Count;

                                if (pluginContext.Topic.Context.Result.TerminateHost)
                                {
                                    pluginContext.OnLog(LogSeverity.Error, false, null, null, "requested to terminate the execution of the module");

                                    result = ExecutionResult.PluginFailedAndExecutionTerminated;
                                    pluginContext.Finish();
                                    pluginContext.Topic.Context.Close();
                                    break; // stop processing plugins
                                }

                                if (!pluginContext.Topic.Context.Result.Success)
                                {
                                    result = ExecutionResult.PluginFailed;
                                }
                            }
                            catch (Exception ex)
                            {
                                result = ExecutionResult.PluginFailedAndExecutionTerminated;
                                pluginContext.Finish();

                                pluginContext.OnLog(LogSeverity.Error, false, null, null, "unhandled error during plugin execution after {Elapsed}, message: {Message}", pluginContext.RunTime, ex.Message);
                                pluginContext.OnLog(LogSeverity.Error, true, null, null, "requested to terminate the execution of the module: {Message}", ex.Message);

                                pluginContext.Topic.Context.Close();
                                break; // stop processing plugins
                            }
                        }
                        catch (TransactionAbortedException)
                        {
                        }

                        pluginContext.Finish();
                        pluginContext.OnLog(LogSeverity.Information, false, null, null, "plugin finished in {Elapsed}", pluginContext.RunTime);
                        LogPluginCounters(pluginContext);

                        pluginContext.Topic.Context.Close();
                    }
                }

                sessionContext.Finish();

                sessionContext.OnLog(LogSeverity.Information, false, null, null, "-------");
                sessionContext.OnLog(LogSeverity.Information, false, null, null, "SUMMARY");
                sessionContext.OnLog(LogSeverity.Information, false, null, null, "-------");

                var longestPluginName = contextList.Max(x => x.PluginName.Length);

                foreach (var pluginContext in contextList)
                {
                    if (pluginContext.Topic.Context == null)
                        continue;

                    LogPluginSummary(sessionContext, pluginContext, longestPluginName);
                }

                //sessionContext.Finish();

                LogSessionSummary(sessionContext, longestPluginName, sessionExceptionCount, sessionWarningCount, sessionStartedOn, result);

                sessionContext.OnContextClosed();
            }
            catch (TransactionAbortedException)
            {
            }

            return result;
        }

        private static void LogPluginCounters(ExecutionContext pluginContext)
        {
            if (pluginContext.IoCommandCounters.Count == 0)
                return;

            var maxKeyLength = pluginContext.IoCommandCounters.Max(x => x.Key.ToString().Length);
            var maxInvocationLength = pluginContext.IoCommandCounters.Max(x => x.Value.InvocationCount.ToString(ValueFormatter.DefaultIntegerFormat, CultureInfo.InvariantCulture).Length);

            foreach (var kvp in pluginContext.IoCommandCounters.OrderBy(kvp => kvp.Key.ToString()))
            {
                if (kvp.Value.AffectedDataCount != null)
                {
                    pluginContext.OnLog(LogSeverity.Debug, false, null, null, "{Kind}{spacing1} {InvocationCount}{spacing2}   {AffectedDataCount}", kvp.Key.ToString(),
                        "".PadRight(maxKeyLength - kvp.Key.ToString().Length, ' '),
                        kvp.Value.InvocationCount,
                        "".PadRight(maxInvocationLength - kvp.Value.InvocationCount.ToString(ValueFormatter.DefaultIntegerFormat, CultureInfo.InvariantCulture).Length, ' '),
                        kvp.Value.AffectedDataCount);
                }
                else
                {
                    pluginContext.OnLog(LogSeverity.Debug, false, null, null, "{Kind}{spacing1} {InvocationCount}", kvp.Key.ToString(),
                        "".PadRight(maxKeyLength - kvp.Key.ToString().Length, ' '), kvp.Value.InvocationCount);
                }
            }
        }

        private static void LogPluginSummary(ExecutionContext sessionContext, ExecutionContext pluginContext, int longestPluginName)
        {
            var spacing1 = "".PadRight(longestPluginName - pluginContext.PluginName.Length);
            var spacing1WithoutName = "".PadRight(longestPluginName);

            if (pluginContext.Topic.Context.Result.Success)
            {
                sessionContext.OnLog(LogSeverity.Information, false, null, null, "{Plugin}{spacing1} run-time is {Elapsed}, result is {Result}, CPU time: {CpuTime}, total allocations: {AllocatedMemory}, allocation difference: {MemoryDifference}", pluginContext.PluginName,
                    spacing1, pluginContext.RunTime, "success", pluginContext.CpuTime, pluginContext.TotalAllocations, pluginContext.AllocationDifference);
            }
            else
            {
                sessionContext.OnLog(LogSeverity.Information, false, null, null, "{Plugin}{spacing1} run-time is {Elapsed}, result is {Result}, requested to terminate execution: {TerminateHost}, CPU time: {CpuTime}, total allocations: {AllocatedMemory}, allocation difference: {MemoryDifference}", pluginContext.PluginName,
                    spacing1, pluginContext.RunTime, "failed", pluginContext.Topic.Context.Result.TerminateHost, pluginContext.CpuTime, pluginContext.TotalAllocations, pluginContext.AllocationDifference);
            }

            if (pluginContext.IoCommandCounters.Count > 0)
            {
                var maxKeyLength = pluginContext.IoCommandCounters.Max(x => x.Key.ToString().Length);
                var maxInvocationLength = pluginContext.IoCommandCounters.Max(x => x.Value.InvocationCount.ToString(ValueFormatter.DefaultIntegerFormat, CultureInfo.InvariantCulture).Length);

                foreach (var kvp in pluginContext.IoCommandCounters.OrderBy(kvp => kvp.Key.ToString()))
                {
                    if (kvp.Value.AffectedDataCount != null)
                    {
                        sessionContext.OnLog(LogSeverity.Information, false, null, null, "{spacing1} {Kind}{spacing2} {InvocationCount}{spacing3}   {AffectedDataCount}", spacing1WithoutName,
                            kvp.Key.ToString(),
                            "".PadRight(maxKeyLength - kvp.Key.ToString().Length, ' '),
                            kvp.Value.InvocationCount,
                            "".PadRight(maxInvocationLength - kvp.Value.InvocationCount.ToString(ValueFormatter.DefaultIntegerFormat, CultureInfo.InvariantCulture).Length, ' '),
                            kvp.Value.AffectedDataCount);
                    }
                    else
                    {
                        sessionContext.OnLog(LogSeverity.Information, false, null, null, "{spacing1} {Kind}{spacing2} {InvocationCount}", spacing1WithoutName,
                            kvp.Key.ToString(),
                            "".PadRight(maxKeyLength - kvp.Key.ToString().Length, ' '),
                            kvp.Value.InvocationCount);
                    }
                }
            }
        }

        private static void LogSessionSummary(ExecutionContext sessionContext, int longestPluginName, int sessionExceptionCount, int sessionWarningCount, Stopwatch sessionStartedOn, ExecutionResult result)
        {
            var sessionName = "SESSION";
            var spacing1 = "".PadRight(longestPluginName - sessionName.Length);
            var spacing1withoutName = "".PadRight(longestPluginName);

            sessionContext.OnLog(LogSeverity.Information, false, null, null, "{Plugin}{spacing1} run-time is {Elapsed}, result is {Result}, CPU time: {CpuTime}, total allocations: {AllocatedMemory}, allocation difference: {MemoryDifference}", sessionName,
                spacing1,
                sessionStartedOn.Elapsed,
                result.ToString(), sessionContext.CpuTime, sessionContext.TotalAllocations, sessionContext.AllocationDifference);

            if (sessionWarningCount > 0)
            {
                sessionContext.OnLog(LogSeverity.Warning, false, null, null, "{spacing1} {Count} warnings/errors occured", spacing1withoutName,
                    sessionWarningCount);
            }

            if (sessionExceptionCount > 0)
            {
                sessionContext.OnLog(LogSeverity.Warning, false, null, null, "{spacing1} {Count} exceptions raised", spacing1withoutName,
                    sessionExceptionCount);
            }

            if (sessionContext.IoCommandCounters.Count > 0)
            {
                var maxKeyLength = sessionContext.IoCommandCounters.Max(x => x.Key.ToString().Length);
                var maxInvocationLength = sessionContext.IoCommandCounters.Max(x => x.Value.InvocationCount.ToString(ValueFormatter.DefaultIntegerFormat, CultureInfo.InvariantCulture).Length);

                foreach (var kvp in sessionContext.IoCommandCounters.OrderBy(kvp => kvp.Key.ToString()))
                {
                    if (kvp.Value.AffectedDataCount != null)
                    {
                        sessionContext.OnLog(LogSeverity.Information, false, null, null, "{spacing1} {Kind}{spacing2} {InvocationCount}{spacing3}   {AffectedDataCount}", spacing1withoutName,
                            kvp.Key.ToString(),
                            "".PadRight(maxKeyLength - kvp.Key.ToString().Length, ' '),
                            kvp.Value.InvocationCount,
                            "".PadRight(maxInvocationLength - kvp.Value.InvocationCount.ToString(ValueFormatter.DefaultIntegerFormat, CultureInfo.InvariantCulture).Length, ' '),
                            kvp.Value.AffectedDataCount);
                    }
                    else
                    {
                        sessionContext.OnLog(LogSeverity.Information, false, null, null, "{spacing1} {Kind}{spacing2} {InvocationCount}", spacing1withoutName,
                            kvp.Key.ToString(),
                            "".PadRight(maxKeyLength - kvp.Key.ToString().Length, ' '),
                            kvp.Value.InvocationCount);
                    }
                }
            }
        }
    }
}