namespace FizzCode.EtLast.ConsoleHost;

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Transactions;
using FizzCode.EtLast;
using FizzCode.EtLast.ConsoleHost.SerilogSink;
using Serilog;
using Serilog.Events;
using Serilog.Formatting.Compact;

internal static class ModuleExecuter
{
    public static ExecutionResult Execute(CommandContext commandContext, CompiledModule module, string[] commands)
    {
        var result = ExecutionResult.Success;

        var etlContext = new EtlContext();

        try
        {
            var listeners = commandContext.HostConfiguration.GetSessionListeners(null);
            if (listeners?.Count > 0)
            {
                etlContext.Listeners.AddRange(listeners);
            }
        }
        catch (Exception ex)
        {
            var formattedMessage = ex.FormatExceptionWithDetails();
            etlContext.Log(LogSeverity.Fatal, null, "{ErrorMessage}", formattedMessage);
            etlContext.LogOps(LogSeverity.Fatal, null, "{ErrorMessage}", formattedMessage);
        }

        var instance = Environment.MachineName;
        var arguments = GetArguments(module, instance);

        var environmentSettings = new EnvironmentSettings();
        module.Startup.Configure(environmentSettings);
        var taskCreators = new Dictionary<string, Func<IEtlSessionArguments, IEtlTask>>(module.Startup.Commands, StringComparer.InvariantCultureIgnoreCase);

        var sessionId = "s" + DateTime.Now.ToString("yyMMdd-HHmmss-ff", CultureInfo.InvariantCulture);
        var session = new EtlSession(sessionId, etlContext, arguments);

        etlContext.TransactionScopeTimeout = environmentSettings.TransactionScopeTimeout;

        var logger = CreateLogger(environmentSettings, commandContext.DevLogFolder, commandContext.OpsLogFolder);
        var opsLogger = CreateOpsLogger(environmentSettings, commandContext.DevLogFolder, commandContext.OpsLogFolder);
        var ioLogger = CreateIoLogger(environmentSettings, commandContext.DevLogFolder, commandContext.OpsLogFolder);

        var serilogAdapter = new EtlSessionSerilogAdapter(logger, opsLogger, ioLogger, commandContext.DevLogFolder, commandContext.OpsLogFolder);
        etlContext.Listeners.Add(serilogAdapter);

        serilogAdapter.Log(LogSeverity.Information, false, null, null, "session {SessionId} started", sessionId);

        if (!string.IsNullOrEmpty(environmentSettings.SeqSettings.Url))
        {
            etlContext.Log(LogSeverity.Debug, null, "all session logs will be sent to SEQ listening on {SeqUrl}", environmentSettings.SeqSettings.Url);
        }

        var sessionStartedOn = Stopwatch.StartNew();
        var sessionExceptionCount = 0;

        var taskResults = new List<KeyValuePair<IEtlTask, ProcessResult>>();

        try
        {
            foreach (var command in commands)
            {
                IEtlTask task = null;
                if (taskCreators.TryGetValue(command, out var taskCreator))
                {
                    task = taskCreator.Invoke(arguments);
                }
                else
                {
                    var taskType = module.TaskTypes.Find(x => string.Equals(x.Name, command, StringComparison.InvariantCultureIgnoreCase));
                    if (taskType != null)
                        task = (IEtlTask)Activator.CreateInstance(taskType);
                }

                if (task == null)
                {
                    serilogAdapter.Log(LogSeverity.Error, false, null, null, "unknown task/flow type: " + command);
                    break;
                }

                try
                {
                    try
                    {
                        var taskResult = session.ExecuteTask(null, task);
                        taskResults.Add(new KeyValuePair<IEtlTask, ProcessResult>(task, taskResult));

                        sessionExceptionCount += taskResult.ExceptionCount;

                        if (sessionExceptionCount > 0)
                        {
                            etlContext.Log(LogSeverity.Error, task, "failed, terminating execution");
                            result = ExecutionResult.ExecutionFailed;
                            etlContext.Close();
                            break; // stop processing tasks
                        }
                    }
                    catch (Exception ex)
                    {
                        etlContext.Log(LogSeverity.Error, task, "failed, terminating execution, reason: ", task.Statistics.RunTime, ex.Message);
                        result = ExecutionResult.ExecutionFailed;
                        etlContext.Close();
                        break; // stop processing tasks
                    }
                }
                catch (TransactionAbortedException)
                {
                }

                LogTaskCounters(serilogAdapter, task);
            }

            session.Stop();

            if (taskResults.Count > 0)
            {
                serilogAdapter.Log(LogSeverity.Information, false, null, null, "-------");
                serilogAdapter.Log(LogSeverity.Information, false, null, null, "SUMMARY");
                serilogAdapter.Log(LogSeverity.Information, false, null, null, "-------");

                var longestTaskName = taskResults.Max(x => x.Key.Name.Length);
                foreach (var kvp in taskResults)
                {
                    LogTaskSummary(serilogAdapter, kvp.Key, kvp.Value, longestTaskName);
                }
            }

            etlContext.Close();
        }
        catch (TransactionAbortedException)
        {
        }

        return result;
    }

    private static EtlSessionArguments GetArguments(CompiledModule module, string instance)
    {
        var argumentValues = new Dictionary<string, object>(StringComparer.InvariantCultureIgnoreCase);

        foreach (var provider in module.DefaultArgumentProviders)
        {
            var values = provider.Arguments;
            if (values != null)
            {
                foreach (var kvp in values)
                    argumentValues[kvp.Key] = kvp.Value;
            }
        }

        foreach (var provider in module.InstanceArgumentProviders.Where(x => string.Equals(x.Instance, instance, StringComparison.InvariantCultureIgnoreCase)))
        {
            var values = provider.Arguments;
            if (values != null)
            {
                foreach (var kvp in values)
                    argumentValues[kvp.Key] = kvp.Value;
            }
        }

        return new EtlSessionArguments(argumentValues);
    }

    private static void LogTaskCounters(EtlSessionSerilogAdapter serilogAdapter, IEtlTask task)
    {
        if (task.IoCommandCounters.Count == 0)
            return;

        const string kind = "kind";
        const string invocation = "invoc.";
        const string affected = "affected";

        var maxKeyLength = Math.Max(kind.Length, task.IoCommandCounters.Max(x => x.Key.ToString().Length));
        var maxInvocationLength = Math.Max(invocation.Length, task.IoCommandCounters.Max(x => x.Value.InvocationCount.ToString(SerilogSink.ValueFormatter.DefaultIntegerFormat, CultureInfo.InvariantCulture).Length));

        serilogAdapter.Log(LogSeverity.Debug, false, null, null, "{Kind}{spacing1} {InvocationCount}{spacing2}   {AffectedDataCount}", kind,
            "".PadRight(maxKeyLength - kind.Length, ' '),
            invocation,
            "".PadRight(maxInvocationLength - invocation.Length, ' '),
            affected);

        foreach (var kvp in task.IoCommandCounters.OrderBy(kvp => kvp.Key.ToString()))
        {
            if (kvp.Value.AffectedDataCount != null)
            {
                serilogAdapter.Log(LogSeverity.Debug, false, null, null, "{Kind}{spacing1} {InvocationCount}{spacing2}   {AffectedDataCount}", kvp.Key.ToString(),
                    "".PadRight(maxKeyLength - kvp.Key.ToString().Length, ' '),
                    kvp.Value.InvocationCount,
                    "".PadRight(maxInvocationLength - kvp.Value.InvocationCount.ToString(SerilogSink.ValueFormatter.DefaultIntegerFormat, CultureInfo.InvariantCulture).Length, ' '),
                    kvp.Value.AffectedDataCount);
            }
            else
            {
                serilogAdapter.Log(LogSeverity.Debug, false, null, null, "{Kind}{spacing1} {InvocationCount}", kvp.Key.ToString(),
                    "".PadRight(maxKeyLength - kvp.Key.ToString().Length, ' '), kvp.Value.InvocationCount);
            }
        }
    }

    private static void LogTaskSummary(EtlSessionSerilogAdapter serilogAdapter, IEtlTask task, ProcessResult result, int longestTaskName)
    {
        var spacing1 = "".PadRight(longestTaskName - task.Name.Length);
        var spacing1WithoutName = "".PadRight(longestTaskName);

        if (result.ExceptionCount == 0)
        {
            serilogAdapter.Log(LogSeverity.Information, false, null, null, "{Task}{spacing1} run-time is {Elapsed}, result is {Result}, CPU time: {CpuTime}, total allocations: {AllocatedMemory}, allocation difference: {MemoryDifference}",
                task.Name, spacing1, task.Statistics.RunTime, "success", task.Statistics.CpuTime, task.Statistics.TotalAllocations, task.Statistics.AllocationDifference);
        }
        else
        {
            serilogAdapter.Log(LogSeverity.Information, false, null, null, "{Task}{spacing1} run-time is {Elapsed}, result is {Result}, CPU time: {CpuTime}, total allocations: {AllocatedMemory}, allocation difference: {MemoryDifference}",
                task.Name, spacing1, task.Statistics.RunTime, "failed", task.Statistics.CpuTime, task.Statistics.TotalAllocations, task.Statistics.AllocationDifference);
        }

        if (task.IoCommandCounters.Count > 0)
        {
            var maxKeyLength = task.IoCommandCounters.Max(x => x.Key.ToString().Length);
            var maxInvocationLength = task.IoCommandCounters.Max(x => x.Value.InvocationCount.ToString(SerilogSink.ValueFormatter.DefaultIntegerFormat, CultureInfo.InvariantCulture).Length);

            foreach (var kvp in task.IoCommandCounters.OrderBy(kvp => kvp.Key.ToString()))
            {
                if (kvp.Value.AffectedDataCount != null)
                {
                    serilogAdapter.Log(LogSeverity.Information, false, null, null, "{spacing1} {Kind}{spacing2} {InvocationCount}{spacing3}   {AffectedDataCount}", spacing1WithoutName,
                        kvp.Key.ToString(),
                        "".PadRight(maxKeyLength - kvp.Key.ToString().Length, ' '),
                        kvp.Value.InvocationCount,
                        "".PadRight(maxInvocationLength - kvp.Value.InvocationCount.ToString(SerilogSink.ValueFormatter.DefaultIntegerFormat, CultureInfo.InvariantCulture).Length, ' '),
                        kvp.Value.AffectedDataCount);
                }
                else
                {
                    serilogAdapter.Log(LogSeverity.Information, false, null, null, "{spacing1} {Kind}{spacing2} {InvocationCount}", spacing1WithoutName,
                        kvp.Key.ToString(),
                        "".PadRight(maxKeyLength - kvp.Key.ToString().Length, ' '),
                        kvp.Value.InvocationCount);
                }
            }
        }
    }

    public static ILogger CreateLogger(EnvironmentSettings settings, string devLogFolder, string opsLogFolder)
    {
        var config = new LoggerConfiguration()
            .WriteTo.File(new CompactJsonFormatter(), Path.Combine(devLogFolder, "events-.json"),
                restrictedToMinimumLevel: (LogEventLevel)settings.FileLogSettings.MinimumLogLevel,
                retainedFileCountLimit: settings.FileLogSettings.RetainSettings.InfoFileCount,
                rollingInterval: RollingInterval.Day,
                encoding: Encoding.UTF8)

            .WriteTo.File(Path.Combine(devLogFolder, "2-info-.txt"),
                restrictedToMinimumLevel: LogEventLevel.Information,
                retainedFileCountLimit: settings.FileLogSettings.RetainSettings.InfoFileCount,
                outputTemplate: "{Timestamp:HH:mm:ss.fff zzz} [{Level:u3}] {Message:l} {NewLine}{Exception}",
                rollingInterval: RollingInterval.Day,
                formatProvider: CultureInfo.InvariantCulture,
                encoding: Encoding.UTF8)

            .WriteTo.File(Path.Combine(devLogFolder, "3-warning-.txt"),
                restrictedToMinimumLevel: LogEventLevel.Warning,
                retainedFileCountLimit: settings.FileLogSettings.RetainSettings.ImportantFileCount,
                outputTemplate: "{Timestamp:HH:mm:ss.fff zzz} [{Level:u3}] {Message:l} {NewLine}{Exception}",
                rollingInterval: RollingInterval.Day,
                formatProvider: CultureInfo.InvariantCulture,
                encoding: Encoding.UTF8)

            .WriteTo.File(Path.Combine(devLogFolder, "4-error-.txt"),
                restrictedToMinimumLevel: LogEventLevel.Error,
                retainedFileCountLimit: settings.FileLogSettings.RetainSettings.ImportantFileCount,
                outputTemplate: "{Timestamp:HH:mm:ss.fff zzz} [{Level:u3}] {Message:l} {NewLine}{Exception}",
                rollingInterval: RollingInterval.Day,
                formatProvider: CultureInfo.InvariantCulture,
                encoding: Encoding.UTF8)

            .WriteTo.File(Path.Combine(devLogFolder, "5-fatal-.txt"),
                restrictedToMinimumLevel: LogEventLevel.Fatal,
                retainedFileCountLimit: settings.FileLogSettings.RetainSettings.ImportantFileCount,
                outputTemplate: "{Timestamp:HH:mm:ss.fff zzz} [{Level:u3}] {Message:l} {NewLine}{Exception}",
                rollingInterval: RollingInterval.Day,
                formatProvider: CultureInfo.InvariantCulture,
                encoding: Encoding.UTF8);

        if (settings.FileLogSettings.MinimumLogLevel <= LogSeverity.Debug)
        {
            config.WriteTo.File(Path.Combine(devLogFolder, "1-debug-.txt"),
                restrictedToMinimumLevel: LogEventLevel.Debug,
                retainedFileCountLimit: settings.FileLogSettings.RetainSettings.LowFileCount,
                outputTemplate: "{Timestamp:HH:mm:ss.fff zzz} [{Level:u3}] {Message:l} {NewLine}{Exception}",
                rollingInterval: RollingInterval.Day,
                formatProvider: CultureInfo.InvariantCulture,
                encoding: Encoding.UTF8);
        }

        if (settings.FileLogSettings.MinimumLogLevel <= LogSeverity.Verbose)
        {
            config.WriteTo.File(Path.Combine(devLogFolder, "0-verbose-.txt"),
                restrictedToMinimumLevel: LogEventLevel.Verbose,
                retainedFileCountLimit: settings.FileLogSettings.RetainSettings.LowFileCount,
                outputTemplate: "{Timestamp:HH:mm:ss.fff zzz} [{Level:u3}] {Message:l} {NewLine}{Exception}",
                rollingInterval: RollingInterval.Day,
                formatProvider: CultureInfo.InvariantCulture,
                encoding: Encoding.UTF8);
        }

        config.WriteTo.Sink(new ConsoleSink("{Timestamp:HH:mm:ss.fff} [{Level}] {Message} {Properties}{NewLine}{Exception}"),
            (LogEventLevel)settings.ConsoleLogSettings.MinimumLogLevel);

        config = config.MinimumLevel.Is(Debugger.IsAttached ? LogEventLevel.Verbose : LogEventLevel.Debug);

        if (!string.IsNullOrEmpty(settings.SeqSettings.Url))
        {
            config = config.WriteTo.Seq(settings.SeqSettings.Url, apiKey: settings.SeqSettings.ApiKey);
        }

        return config.CreateLogger();
    }

    public static ILogger CreateOpsLogger(EnvironmentSettings settings, string devLogFolder, string opsLogFolder)
    {
        var config = new LoggerConfiguration()
            .WriteTo.File(Path.Combine(opsLogFolder, "2-info-.txt"),
                restrictedToMinimumLevel: LogEventLevel.Information,
                retainedFileCountLimit: settings.FileLogSettings.RetainSettings.InfoFileCount,
                outputTemplate: "{Timestamp:HH:mm:ss.fff zzz} [{Level:u3}] {Message:l} {NewLine}{Exception}",
                rollingInterval: RollingInterval.Day,
                formatProvider: CultureInfo.InvariantCulture,
                encoding: Encoding.UTF8)

            .WriteTo.File(Path.Combine(opsLogFolder, "3-warning-.txt"),
                restrictedToMinimumLevel: LogEventLevel.Warning,
                retainedFileCountLimit: settings.FileLogSettings.RetainSettings.ImportantFileCount,
                outputTemplate: "{Timestamp:HH:mm:ss.fff zzz} [{Level:u3}] {Message:l} {NewLine}{Exception}",
                rollingInterval: RollingInterval.Day,
                formatProvider: CultureInfo.InvariantCulture,
                encoding: Encoding.UTF8)

            .WriteTo.File(Path.Combine(opsLogFolder, "4-error-.txt"),
                restrictedToMinimumLevel: LogEventLevel.Error,
                retainedFileCountLimit: settings.FileLogSettings.RetainSettings.ImportantFileCount,
                outputTemplate: "{Timestamp:HH:mm:ss.fff zzz} [{Level:u3}] {Message:l} {NewLine}{Exception}",
                rollingInterval: RollingInterval.Day,
                formatProvider: CultureInfo.InvariantCulture,
                encoding: Encoding.UTF8)

            .WriteTo.File(Path.Combine(opsLogFolder, "5-fatal-.txt"),
                restrictedToMinimumLevel: LogEventLevel.Fatal,
                retainedFileCountLimit: settings.FileLogSettings.RetainSettings.ImportantFileCount,
                outputTemplate: "{Timestamp:HH:mm:ss.fff zzz} [{Level:u3}] {Message:l} {NewLine}{Exception}",
                rollingInterval: RollingInterval.Day,
                formatProvider: CultureInfo.InvariantCulture,
                encoding: Encoding.UTF8);

        config = config.MinimumLevel.Is(LogEventLevel.Information);

        return config.CreateLogger();
    }

    public static ILogger CreateIoLogger(EnvironmentSettings settings, string devLogFolder, string opsLogFolder)
    {
        var config = new LoggerConfiguration()
            .WriteTo.File(Path.Combine(devLogFolder, "io-.txt"),
                restrictedToMinimumLevel: (LogEventLevel)settings.FileLogSettings.MinimumLogLevelIo,
                retainedFileCountLimit: settings.FileLogSettings.RetainSettings.LowFileCount,
                outputTemplate: "{Timestamp:HH:mm:ss.fff zzz} [{Level:u3}] {Message:l} {NewLine}{Exception}",
                rollingInterval: RollingInterval.Day,
                formatProvider: CultureInfo.InvariantCulture,
                encoding: Encoding.UTF8);

        config.WriteTo.Sink(new ConsoleSink("{Timestamp:HH:mm:ss.fff} [{Level}] {Message} {Properties}{NewLine}{Exception}"),
            (LogEventLevel)settings.ConsoleLogSettings.MinimumLogLevel);

        config = config.MinimumLevel.Is(LogEventLevel.Verbose);

        if (!string.IsNullOrEmpty(settings.SeqSettings.Url))
        {
            config = config.WriteTo.Seq(settings.SeqSettings.Url, apiKey: settings.SeqSettings.ApiKey);
        }

        return config.CreateLogger();
    }
}
