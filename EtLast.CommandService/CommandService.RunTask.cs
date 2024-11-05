namespace FizzCode.EtLast;

public partial class CommandService
{
    protected override IExecutionResult RunTasks(string commandId, string moduleName, StartupDelegate startup, List<IEtlTask> tasks, ArgumentCollection arguments)
    {
        var executionResult = new ExecutionResult();
        var tasksDirectoryName = string.Join('+', tasks.Select(task => string.Join("_", task.GetType().Name.Split(Path.GetInvalidFileNameChars()))));

        string currentDevLogDirectory;
        string currentOpsLogDirectory;
        if (moduleName != null)
        {
            var moduleDirectoryName = string.Join("_", moduleName.Split(Path.GetInvalidFileNameChars()));
            currentDevLogDirectory = Path.Combine(DevLogDirectory, moduleDirectoryName, tasksDirectoryName);
            currentOpsLogDirectory = Path.Combine(OpsLogDirectory, moduleDirectoryName, tasksDirectoryName);
        }
        else
        {
            currentDevLogDirectory = Path.Combine(DevLogDirectory, tasksDirectoryName);
            currentOpsLogDirectory = Path.Combine(OpsLogDirectory, tasksDirectoryName);
        }

        var contextName = string.Join('+', tasks.Select(task => string.Join("_", task.GetType().Name.Split(Path.GetInvalidFileNameChars()))));
        var context = new EtlContext(arguments, contextName, commandId);

        executionResult.ContextManifest = context.Manifest;

        var sessionBuilder = new SessionBuilder()
        {
            Context = context,
            TasksDirectoryName = tasksDirectoryName,
            DevLogDirectory = currentDevLogDirectory,
            OpsLogDirectory = currentOpsLogDirectory,
            Tasks = tasks.ToArray(),
        };

        startup?.Invoke(sessionBuilder, arguments);

        foreach (var configurator in SessionConfigurators)
            configurator?.Invoke(sessionBuilder, arguments);

        if (moduleName != null)
            context.Manifest.Extra["ModuleName"] = moduleName;

        context.Manifest.Extra["TaskNames"] = tasks.Select(x => x.GetType().GetFriendlyTypeName()).ToArray();

        foreach (var manifestProcessor in sessionBuilder.ManifestProcessors)
            manifestProcessor?.RegisterToManifestEvents(context, context.Manifest);

        try
        {
            if (EtlContextListenerCreators?.Count > 0)
            {
                foreach (var listenerCreator in EtlContextListenerCreators)
                {
                    if (listenerCreator != null)
                    {
                        var listener = listenerCreator.Invoke(context);
                        listener.Start();
                        context.Listeners.Add(listener);
                    }
                }
            }
        }
        catch (Exception ex)
        {
            var formattedMessage = ex.FormatExceptionWithDetails();
            context.Log(LogSeverity.Error, null, "{ErrorMessage}", formattedMessage);
            context.LogOps(LogSeverity.Error, null, "{ErrorMessage}", formattedMessage);
        }

        context.Log(LogSeverity.Information, null, "context {ContextName} started with ID: {ContextId}", context.Manifest.ContextName, context.Manifest.ContextId);

        var startedOn = Stopwatch.StartNew();
        var taskResults = new List<TaskExecutionResult>();
        var flowState = new FlowState(context);

        try
        {
            foreach (var task in tasks)
            {
                if (flowState.IsTerminating)
                    break;

                try
                {
                    task.Execute(context, flowState);

                    taskResults.Add(new TaskExecutionResult(task));
                    executionResult.TaskResults.Add(new TaskExecutionResult(task));

                    if (flowState.Failed)
                    {
                        context.Log(LogSeverity.Fatal, task, "failed, terminating execution");
                        executionResult.Status = ExecutionStatusCode.ExecutionFailed;
                        context.Close();
                        break; // stop processing tasks
                    }
                }
                catch (Exception ex)
                {
                    context.Log(LogSeverity.Fatal, task, "failed, terminating execution, reason: {0}", ex.Message);
                    executionResult.Status = ExecutionStatusCode.ExecutionFailed;
                    context.Close();
                    break; // stop processing tasks
                }

                LogTaskCounters(context, task);
            }

            context.StopServices();

            var scopeActions = context.GetScopeActions();
            if (scopeActions?.Length > 0)
            {
                context.Log(LogSeverity.Information, null, "-------------");
                context.Log(LogSeverity.Information, null, "SCOPE ACTIONS");
                context.Log(LogSeverity.Information, null, "-------------");
                var sb = new StringBuilder();
                var args = new List<object>();
                foreach (var action in scopeActions)
                {
                    sb.Clear();
                    args.Clear();

                    if (action.Caller is IProcess callerProcess)
                    {
                        var typ = callerProcess is IEtlTask ? "Task" : "Process";
                        sb.Append("in {Active").Append(typ).Append("} ");
                        args.Add(callerProcess.UniqueName);
                    }

                    sb.Append("is {Action}");
                    args.Add(action.Action);

                    if (action.Process != null)
                    {
                        var typ = action.Process is IEtlTask ? "Task" : "Process";
                        sb.Append(" by {").Append(typ).Append("}, {ProcessType}");
                        args.Add(action.Process.Name);
                        args.Add(action.Process.GetType().GetFriendlyTypeName());
                    }

                    context.Log(LogSeverity.Information, null, sb.ToString(), [.. args]);
                }

                if (taskResults.Count > 0)
                {
                    context.Log(LogSeverity.Information, null, "-------");
                    context.Log(LogSeverity.Information, null, "SUMMARY");
                    context.Log(LogSeverity.Information, null, "-------");

                    var longestTaskName = taskResults.Max(x => x.TaskName.Length);
                    foreach (var taskResult in taskResults)
                    {
                        LogTaskSummary(context, taskResult, longestTaskName);
                    }
                }
            }

            context.Close();
        }
        catch (TransactionAbortedException)
        {
        }

        return executionResult;
    }

    private static void LogTaskCounters(IEtlContext context, IEtlTask task)
    {
        if (!task.IoCommandCounters.Any())
            return;

        const string kind = "kind";
        const string invocation = "invoc.";
        const string affected = "affected";

        var maxKeyLength = Math.Max(kind.Length, task.IoCommandCounters.Max(x => x.Kind.ToString().Length));
        var maxInvocationLength = Math.Max(invocation.Length, task.IoCommandCounters.Max(x => x.InvocationCount.ToString(SinkValueFormatter.DefaultIntegerFormat, CultureInfo.InvariantCulture).Length));

        context.Log(LogSeverity.Debug, task, "{Kind}{spacing1} {InvocationCount}{spacing2}   {AffectedDataCount}", kind,
            "".PadRight(maxKeyLength - kind.Length, ' '),
            invocation,
            "".PadRight(maxInvocationLength - invocation.Length, ' '),
            affected);

        foreach (var kvp in task.IoCommandCounters.OrderBy(x => x.Kind.ToString()))
        {
            if (kvp.AffectedDataCount != null)
            {
                context.Log(LogSeverity.Debug, task, "{Kind}{spacing1} {InvocationCount}{spacing2}   {AffectedDataCount}", kvp.Kind.ToString(),
                    "".PadRight(maxKeyLength - kvp.Kind.ToString().Length, ' '),
                    kvp.InvocationCount,
                    "".PadRight(maxInvocationLength - kvp.InvocationCount.ToString(SinkValueFormatter.DefaultIntegerFormat, CultureInfo.InvariantCulture).Length, ' '),
                    kvp.AffectedDataCount);
            }
            else
            {
                context.Log(LogSeverity.Debug, task, "{Kind}{spacing1} {InvocationCount}", kvp.Kind.ToString(),
                    "".PadRight(maxKeyLength - kvp.Kind.ToString().Length, ' '), kvp.InvocationCount);
            }
        }
    }

    private static void LogTaskSummary(IEtlContext context, TaskExecutionResult result, int longestTaskName)
    {
        var spacing1 = "".PadRight(longestTaskName - result.TaskName.Length);
        var spacing1WithoutName = "".PadRight(longestTaskName);

        if (result.Exceptions.Count == 0)
        {
            context.Log(LogSeverity.Information, null, "{Task}{spacing1} run-time is {Elapsed}, result is {ProcessResult}, CPU time: {CpuTime}, total allocations: {AllocatedMemory}, allocation difference: {MemoryDifference}",
                result.TaskName, spacing1, result.Statistics.RunTime, "success", result.Statistics.CpuTime, result.Statistics.TotalAllocations, result.Statistics.AllocationDifference);
        }
        else
        {
            context.Log(LogSeverity.Information, null, "{Task}{spacing1} run-time is {Elapsed}, result is {ProcessResult}, CPU time: {CpuTime}, total allocations: {AllocatedMemory}, allocation difference: {MemoryDifference}",
                result.TaskName, spacing1, result.Statistics.RunTime, "failed", result.Statistics.CpuTime, result.Statistics.TotalAllocations, result.Statistics.AllocationDifference);
        }

        if (result.IoCommandCounters.Count > 0)
        {
            var maxKeyLength = result.IoCommandCounters.Max(x => x.Kind.ToString().Length);
            var maxInvocationLength = result.IoCommandCounters.Max(x => x.InvocationCount.ToString(SinkValueFormatter.DefaultIntegerFormat, CultureInfo.InvariantCulture).Length);

            foreach (var ioc in result.IoCommandCounters.OrderBy(kvp => kvp.Kind.ToString()))
            {
                if (ioc.AffectedDataCount != null)
                {
                    context.Log(LogSeverity.Information, null, "{spacing1} {Kind}{spacing2} {InvocationCount}{spacing3}   {AffectedDataCount}", spacing1WithoutName,
                        ioc.Kind.ToString(),
                        "".PadRight(maxKeyLength - ioc.Kind.ToString().Length, ' '),
                        ioc.InvocationCount,
                        "".PadRight(maxInvocationLength - ioc.InvocationCount.ToString(SinkValueFormatter.DefaultIntegerFormat, CultureInfo.InvariantCulture).Length, ' '),
                        ioc.AffectedDataCount);
                }
                else
                {
                    context.Log(LogSeverity.Information, null, "{spacing1} {Kind}{spacing2} {InvocationCount}", spacing1WithoutName,
                        ioc.Kind.ToString(),
                        "".PadRight(maxKeyLength - ioc.Kind.ToString().Length, ' '),
                        ioc.InvocationCount);
                }
            }
        }
    }
}
