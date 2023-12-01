namespace FizzCode.EtLast;

internal static class ModuleExecuter
{
    public static IExecutionResult Execute(ConsoleHost host, CompiledModule module, string[] taskNames)
    {
        var executionResult = new ExecutionResult();
        var instance = Environment.MachineName;
        var arguments = new ArgumentCollection(module.DefaultArgumentProviders, module.InstanceArgumentProviders, instance);

        string currentDevLogFolder = null;
        string currentOpsLogFolder = null;
        {
            var moduleFolderName = string.Join("_", module.Name.Split(Path.GetInvalidFileNameChars()));
            var tasksFolderName = string.Join('+', taskNames.Select(taskName => string.Join("_", taskName.Split(Path.GetInvalidFileNameChars()))));

            currentDevLogFolder = Path.Combine(host.DevLogFolder, moduleFolderName, tasksFolderName);
            currentOpsLogFolder = Path.Combine(host.OpsLogFolder, moduleFolderName, tasksFolderName);
        }

        var environmentSettings = new EnvironmentSettings()
        {
            DevLogFolder = currentDevLogFolder,
            OpsLogFolder = currentOpsLogFolder,
        };

        module.Startup?.Configure(environmentSettings);

        if (environmentSettings.LocalManifestLogSettings.Enabled)
        {
            environmentSettings.ManifestProcessors.Add(new ConsoleHostJsonManifestProcessor()
            {
                Folder = Path.Combine(currentDevLogFolder, "manifest"),
                FileNameFunc = manifest => manifest.CreatedOnUtc.ToString("yyyyMMhh-hhmmssfff", CultureInfo.InvariantCulture) + ".json",
            });
        }

        var contextName = string.Join('+', taskNames.Select(taskName => string.Join("_", taskName.Split(Path.GetInvalidFileNameChars()))));
        var context = new EtlContext(arguments, contextName)
        {
            TransactionScopeTimeout = environmentSettings.TransactionScopeTimeout,
        };

        foreach (var manifestProcessor in environmentSettings.ManifestProcessors)
        {
            manifestProcessor.RegisterToManifestEvents(context.Manifest);
        }

        try
        {
            if (host.EtlContextListeners?.Count > 0)
            {
                foreach (var listenerCreator in host.EtlContextListeners)
                {
                    var listener = listenerCreator.Invoke(context);
                    listener.Start();
                    context.Listeners.Add(listener);
                }
            }
        }
        catch (Exception ex)
        {
            var formattedMessage = ex.FormatExceptionWithDetails();
            context.Log(LogSeverity.Error, null, "{ErrorMessage}", formattedMessage);
            context.LogOps(LogSeverity.Error, null, "{ErrorMessage}", formattedMessage);
        }

        if (host.SerilogForModulesEnabled)
        {
            if (environmentSettings.FileLogSettings.Enabled || environmentSettings.ConsoleLogSettings.Enabled || !string.IsNullOrEmpty(environmentSettings.SeqSettings.Url))
            {
                var serilogAdapter = new EtlContextSerilogAdapter(environmentSettings, currentDevLogFolder, currentOpsLogFolder);
                context.Listeners.Add(serilogAdapter);
            }
        }

        if (module.Startup == null)
        {
            context.Log(LogSeverity.Warning, null, "Can't find a startup class implementing " + nameof(IStartup) + ".");
        }

        context.Log(LogSeverity.Information, null, "context {ContextName} started with ID: {ContextId}", context.Manifest.ContextName, context.Manifest.ContextId);

        if (!string.IsNullOrEmpty(environmentSettings.SeqSettings.Url))
        {
            context.Log(LogSeverity.Debug, null, "all context logs will be sent to SEQ listening on {SeqUrl}", environmentSettings.SeqSettings.Url);
        }

        var startedOn = Stopwatch.StartNew();

        var taskResults = new List<TaskExectionResult>();

        var flowState = new FlowState(context);

        try
        {
            foreach (var taskName in taskNames)
            {
                if (flowState.IsTerminating)
                    break;

                IEtlTask task = null;

                var taskType = module.TaskTypes.Find(x => string.Equals(x.Name, taskName, StringComparison.InvariantCultureIgnoreCase));
                if (taskType != null)
                    task = (IEtlTask)Activator.CreateInstance(taskType);

                if (task == null)
                {
                    context.Log(LogSeverity.Error, null, "unknown task type: " + taskName);
                    break;
                }

                try
                {
                    task.Execute(context, flowState);

                    taskResults.Add(new TaskExectionResult(task));
                    executionResult.TaskResults.Add(new TaskExectionResult(task));

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
                var topics = scopeActions.Select(x => x.Topic).Distinct().ToArray().OrderBy(x => x);
                var sb = new StringBuilder();
                var args = new List<object>();
                foreach (var topic in topics)
                {
                    var actions = scopeActions.Where(x => x.Topic == topic).ToArray();
                    foreach (var action in actions)
                    {
                        sb.Clear();
                        args.Clear();

                        sb.Append("\tTPC#{ActiveTopic} ");
                        args.Add(action.Topic);
                        if (action.Caller is IProcess callerProcess)
                        {
                            var typ = callerProcess is IEtlTask ? "Task" : "Process";
                            sb.Append("in {Active").Append(typ).Append("} ");
                            args.Add(callerProcess.InvocationName);
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

                        context.Log(LogSeverity.Information, null, sb.ToString(), args.ToArray());
                    }
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
