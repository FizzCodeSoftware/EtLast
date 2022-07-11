namespace FizzCode.EtLast.ConsoleHost;

internal static class ModuleExecuter
{
    public static IExecutionResult Execute(Host host, CompiledModule module, string[] taskNames)
    {
        var executionResult = new ExecutionResult
        {
            TaskResults = new List<TaskExectionResult>(),
        };

        var instance = Environment.MachineName;
        var arguments = new ArgumentCollection(module.DefaultArgumentProviders, module.InstanceArgumentProviders, instance);

        var environmentSettings = new EnvironmentSettings();
        module.Startup.Configure(environmentSettings);
        var customTasks = new Dictionary<string, Func<IArgumentCollection, IEtlTask>>(module.Startup.CustomTasks, StringComparer.InvariantCultureIgnoreCase);

        var sessionId = "s" + DateTime.Now.ToString("yyMMdd-HHmmss-ff", CultureInfo.InvariantCulture);
        var session = new EtlSession(sessionId, arguments);
        session.Context.TransactionScopeTimeout = environmentSettings.TransactionScopeTimeout;

        try
        {
            if (host.EtlContextListeners?.Count > 0)
            {
                foreach (var listenerCreator in host.EtlContextListeners)
                {
                    session.Context.Listeners.Add(listenerCreator.Invoke(session));
                }
            }
        }
        catch (Exception ex)
        {
            var formattedMessage = ex.FormatExceptionWithDetails();
            session.Context.Log(LogSeverity.Fatal, null, "{ErrorMessage}", formattedMessage);
            session.Context.LogOps(LogSeverity.Fatal, null, "{ErrorMessage}", formattedMessage);
        }

        if (host.SerilogForModulesEnabled)
        {
            if (environmentSettings.FileLogSettings.Enabled || environmentSettings.ConsoleLogSettings.Enabled || !string.IsNullOrEmpty(environmentSettings.SeqSettings.Url))
            {
                var serilogAdapter = new EtlSessionSerilogAdapter(environmentSettings, host.DevLogFolder, host.OpsLogFolder);
                session.Context.Listeners.Add(serilogAdapter);
            }
        }

        session.Context.Log(LogSeverity.Information, null, "session {SessionId} started", sessionId);

        if (!string.IsNullOrEmpty(environmentSettings.SeqSettings.Url))
        {
            session.Context.Log(LogSeverity.Debug, null, "all session logs will be sent to SEQ listening on {SeqUrl}", environmentSettings.SeqSettings.Url);
        }

        var sessionStartedOn = Stopwatch.StartNew();
        var sessionExceptions = new List<Exception>();

        var taskResults = new List<TaskExectionResult>();

        try
        {
            foreach (var taskName in taskNames)
            {
                IEtlTask task = null;
                if (customTasks.TryGetValue(taskName, out var taskCreator))
                {
                    task = taskCreator.Invoke(arguments);
                }
                else
                {
                    var taskType = module.TaskTypes.Find(x => string.Equals(x.Name, taskName, StringComparison.InvariantCultureIgnoreCase));
                    if (taskType != null)
                        task = (IEtlTask)Activator.CreateInstance(taskType);
                }

                if (task == null)
                {
                    session.Context.Log(LogSeverity.Error, null, "unknown task/flow type: " + taskName);
                    break;
                }

                try
                {
                    try
                    {
                        var taskResult = session.ExecuteTask(null, task) as ProcessResult;
                        taskResults.Add(new TaskExectionResult(task, taskResult));
                        executionResult.TaskResults.Add(new TaskExectionResult(task, taskResult));

                        sessionExceptions.AddRange(taskResult.Exceptions);

                        if (sessionExceptions.Count > 0)
                        {
                            session.Context.Log(LogSeverity.Error, task, "failed, terminating execution");
                            executionResult.Status = ExecutionStatusCode.ExecutionFailed;
                            session.Context.Close();
                            break; // stop processing tasks
                        }
                    }
                    catch (Exception ex)
                    {
                        session.Context.Log(LogSeverity.Error, task, "failed, terminating execution, reason: {0}", ex.Message);
                        executionResult.Status = ExecutionStatusCode.ExecutionFailed;
                        session.Context.Close();
                        break; // stop processing tasks
                    }
                }
                catch (TransactionAbortedException)
                {
                }

                LogTaskCounters(session.Context, task);
            }

            session.Stop();

            if (taskResults.Count > 0)
            {
                session.Context.Log(LogSeverity.Information, null, "-------");
                session.Context.Log(LogSeverity.Information, null, "SUMMARY");
                session.Context.Log(LogSeverity.Information, null, "-------");

                var longestTaskName = taskResults.Max(x => x.TaskName.Length);
                foreach (var taskResult in taskResults)
                {
                    LogTaskSummary(session.Context, taskResult, longestTaskName);
                }
            }

            session.Context.Close();
        }
        catch (TransactionAbortedException)
        {
        }

        return executionResult;
    }

    private static void LogTaskCounters(IEtlContext context, IEtlTask task)
    {
        if (task.IoCommandCounters.Count == 0)
            return;

        const string kind = "kind";
        const string invocation = "invoc.";
        const string affected = "affected";

        var maxKeyLength = Math.Max(kind.Length, task.IoCommandCounters.Max(x => x.Key.ToString().Length));
        var maxInvocationLength = Math.Max(invocation.Length, task.IoCommandCounters.Max(x => x.Value.InvocationCount.ToString(SerilogSink.ValueFormatter.DefaultIntegerFormat, CultureInfo.InvariantCulture).Length));

        context.Log(LogSeverity.Debug, task, "{Kind}{spacing1} {InvocationCount}{spacing2}   {AffectedDataCount}", kind,
            "".PadRight(maxKeyLength - kind.Length, ' '),
            invocation,
            "".PadRight(maxInvocationLength - invocation.Length, ' '),
            affected);

        foreach (var kvp in task.IoCommandCounters.OrderBy(kvp => kvp.Key.ToString()))
        {
            if (kvp.Value.AffectedDataCount != null)
            {
                context.Log(LogSeverity.Debug, task, "{Kind}{spacing1} {InvocationCount}{spacing2}   {AffectedDataCount}", kvp.Key.ToString(),
                    "".PadRight(maxKeyLength - kvp.Key.ToString().Length, ' '),
                    kvp.Value.InvocationCount,
                    "".PadRight(maxInvocationLength - kvp.Value.InvocationCount.ToString(SerilogSink.ValueFormatter.DefaultIntegerFormat, CultureInfo.InvariantCulture).Length, ' '),
                    kvp.Value.AffectedDataCount);
            }
            else
            {
                context.Log(LogSeverity.Debug, task, "{Kind}{spacing1} {InvocationCount}", kvp.Key.ToString(),
                    "".PadRight(maxKeyLength - kvp.Key.ToString().Length, ' '), kvp.Value.InvocationCount);
            }
        }
    }

    private static void LogTaskSummary(IEtlContext context, TaskExectionResult result, int longestTaskName)
    {
        var spacing1 = "".PadRight(longestTaskName - result.TaskName.Length);
        var spacing1WithoutName = "".PadRight(longestTaskName);

        if (result.Exceptions.Count == 0)
        {
            context.Log(LogSeverity.Information, null, "{Task}{spacing1} run-time is {Elapsed}, result is {Result}, CPU time: {CpuTime}, total allocations: {AllocatedMemory}, allocation difference: {MemoryDifference}",
                result.TaskName, spacing1, result.Statistics.RunTime, "success", result.Statistics.CpuTime, result.Statistics.TotalAllocations, result.Statistics.AllocationDifference);
        }
        else
        {
            context.Log(LogSeverity.Information, null, "{Task}{spacing1} run-time is {Elapsed}, result is {Result}, CPU time: {CpuTime}, total allocations: {AllocatedMemory}, allocation difference: {MemoryDifference}",
                result.TaskName, spacing1, result.Statistics.RunTime, "failed", result.Statistics.CpuTime, result.Statistics.TotalAllocations, result.Statistics.AllocationDifference);
        }

        if (result.IoCommandCounters.Count > 0)
        {
            var maxKeyLength = result.IoCommandCounters.Max(x => x.Key.ToString().Length);
            var maxInvocationLength = result.IoCommandCounters.Max(x => x.Value.InvocationCount.ToString(SerilogSink.ValueFormatter.DefaultIntegerFormat, CultureInfo.InvariantCulture).Length);

            foreach (var kvp in result.IoCommandCounters.OrderBy(kvp => kvp.Key.ToString()))
            {
                if (kvp.Value.AffectedDataCount != null)
                {
                    context.Log(LogSeverity.Information, null, "{spacing1} {Kind}{spacing2} {InvocationCount}{spacing3}   {AffectedDataCount}", spacing1WithoutName,
                        kvp.Key.ToString(),
                        "".PadRight(maxKeyLength - kvp.Key.ToString().Length, ' '),
                        kvp.Value.InvocationCount,
                        "".PadRight(maxInvocationLength - kvp.Value.InvocationCount.ToString(SerilogSink.ValueFormatter.DefaultIntegerFormat, CultureInfo.InvariantCulture).Length, ' '),
                        kvp.Value.AffectedDataCount);
                }
                else
                {
                    context.Log(LogSeverity.Information, null, "{spacing1} {Kind}{spacing2} {InvocationCount}", spacing1WithoutName,
                        kvp.Key.ToString(),
                        "".PadRight(maxKeyLength - kvp.Key.ToString().Length, ' '),
                        kvp.Value.InvocationCount);
                }
            }
        }
    }
}
