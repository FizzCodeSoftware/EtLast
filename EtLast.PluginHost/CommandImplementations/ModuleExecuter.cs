namespace FizzCode.EtLast.PluginHost
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Transactions;
    using Serilog;
    using Serilog.Events;

    public static class ModuleExecuter
    {
        public static ExecutionResult Execute(CommandContext commandContext, string moduleName, string[] moduleSettingOverrides)
        {
            commandContext.Logger.Write(LogEventLevel.Information, "executing module: {ModuleName}", moduleName);

            AppDomain.CurrentDomain.UnhandledException += UnhandledExceptionHandler;
            try
            {
                var module = ModuleLoader.LoadModule(commandContext, moduleName, moduleSettingOverrides);
                if (module == null)
                    return ExecutionResult.ConfigurationError;

                commandContext.Logger.Write(LogEventLevel.Information, "{PluginCount} plugin(s) found: {PluginNames}",
                    module.EnabledPlugins.Count, module.EnabledPlugins.Select(plugin => TypeHelpers.GetFriendlyTypeName(plugin.GetType())).ToArray());

                if (module.EnabledPlugins.Count > 0)
                {
                    var moduleResult = ExecuteModule(commandContext, module);

                    if (moduleResult != ExecutionResult.Success)
                        return moduleResult;
                }

                ModuleLoader.UnloadModule(commandContext, module);
            }
            finally
            {
                AppDomain.CurrentDomain.UnhandledException -= UnhandledExceptionHandler;
            }

            void UnhandledExceptionHandler(object sender, UnhandledExceptionEventArgs e)
            {
                if (commandContext.Logger != null)
                {
                    commandContext.Logger.Error(e.ExceptionObject as Exception, "unexpected error during execution");
                    commandContext.OpsLogger.Error("unexpected error during execution: {Message}", (e.ExceptionObject as Exception)?.Message);
                }
                else
                {
                    Console.WriteLine("unexpected error during execution: " + e.ExceptionObject.ToString());
                }

                Environment.Exit(-1);
            }

            return ExecutionResult.Success;
        }

        private static ExecutionResult ExecuteModule(CommandContext commandContext, Module module)
        {
            var result = ExecutionResult.Success;

            try
            {
                var globalStat = new StatCounterCollection();
                var runTimes = new List<TimeSpan>();
                var pluginResults = new List<EtlContextResult>();
                foreach (var plugin in module.EnabledPlugins)
                {
                    var startedOn = Stopwatch.StartNew();
                    var friendlyPluginName = TypeHelpers.GetFriendlyTypeName(plugin.GetType());
                    commandContext.Logger.Write(LogEventLevel.Information, "executing plugin: {PluginTypeName}", friendlyPluginName);

                    try
                    {
                        try
                        {
                            plugin.Init(commandContext.Logger, commandContext.OpsLogger, module.ModuleConfiguration.Configuration, module.ModuleConfiguration.ModuleFolder, commandContext.HostConfiguration.TransactionScopeTimeout);
                            pluginResults.Add(plugin.Context.Result);

                            plugin.BeforeExecute();
                            plugin.Execute();
                            plugin.AfterExecute();

                            AppendGlobalStat(globalStat, plugin.Context.Stat);

                            if (plugin.Context.Result.TerminateHost)
                            {
                                commandContext.Logger.Write(LogEventLevel.Error, "plugin requested to terminate the execution: {PluginName}", friendlyPluginName);
                                result = ExecutionResult.PluginFailedAndExecutionTerminated;

                                startedOn.Stop();
                                runTimes.Add(startedOn.Elapsed);
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
                            commandContext.Logger.Write(LogEventLevel.Error, ex, "unhandled error during plugin execution after {Elapsed} in {PluginName}", startedOn.Elapsed, friendlyPluginName);
                            commandContext.OpsLogger.Write(LogEventLevel.Error, "unhandled error during plugin execution after {Elapsed} in {PluginName}: {Message}", startedOn.Elapsed, friendlyPluginName, ex.Message);

                            startedOn.Stop();
                            runTimes.Add(startedOn.Elapsed);
                            break; // stop processing plugins
                        }
                    }
                    catch (TransactionAbortedException)
                    {
                    }

                    startedOn.Stop();
                    runTimes.Add(startedOn.Elapsed);

                    commandContext.Logger.Write(LogEventLevel.Information, "execution of {PluginName} is finished in {Elapsed}", friendlyPluginName, startedOn.Elapsed);
                }

                LogStats(globalStat, commandContext.Logger);

                for (var i = 0; i < Math.Min(module.EnabledPlugins.Count, pluginResults.Count); i++)
                {
                    var plugin = module.EnabledPlugins[i];
                    var pluginResult = pluginResults[i];
                    if (pluginResult.Success)
                    {
                        commandContext.Logger.Write(LogEventLevel.Information, "run-time of {PluginName} is {Elapsed}, status is {Status}", TypeHelpers.GetFriendlyTypeName(plugin.GetType()), runTimes[i], "success");
                    }
                    else
                    {
                        commandContext.Logger.Write(LogEventLevel.Information, "run-time of {PluginName} is {Elapsed}, status is {Status}, requested to terminate execution: {TerminateHost}", TypeHelpers.GetFriendlyTypeName(plugin.GetType()), runTimes[i], "failed", pluginResult.TerminateHost);
                    }
                }
            }
            catch (TransactionAbortedException)
            {
            }

            return result;
        }

        private static void AppendGlobalStat(StatCounterCollection globalStat, StatCounterCollection stat)
        {
            foreach (var kvp in stat.GetCountersOrdered())
            {
                if (!kvp.Key.StartsWith(StatCounterCollection.DebugNamePrefix, StringComparison.InvariantCultureIgnoreCase))
                {
                    globalStat.IncrementCounter(kvp.Key, kvp.Value);
                }
            }
        }

        private static void LogStats(StatCounterCollection stats, ILogger logger)
        {
            var counters = stats.GetCountersOrdered();
            if (counters.Count == 0)
                return;

            foreach (var kvp in counters)
            {
                logger.Write(LogEventLevel.Information, "global stat {StatName} = {StatValue}", kvp.Key, kvp.Value);
            }
        }
    }
}