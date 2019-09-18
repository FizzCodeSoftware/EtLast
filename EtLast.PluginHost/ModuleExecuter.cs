﻿namespace FizzCode.EtLast.PluginHost
{
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Diagnostics;
    using System.Transactions;
    using FizzCode.EtLast;
    using Serilog;
    using Serilog.Events;

    internal class ModuleExecuter
    {
        public bool ExecutionTerminated { get; private set; }
        public bool AtLeastOnePluginFailed { get; private set; }

        public void ExecuteModule(PluginHostConfiguration hostConfiguration, List<IEtlPlugin> modulePlugins, ILogger logger, ILogger opsLogger, Configuration pluginConfiguration, string moduleFolder)
        {
            try
            {
                var globalStat = new StatCounterCollection();
                var runTimes = new List<TimeSpan>();
                var pluginResults = new List<EtlContextResult>();
                foreach (var plugin in modulePlugins)
                {
                    var startedOn = Stopwatch.StartNew();
                    logger.Write(LogEventLevel.Information, "executing {PluginTypeName}", plugin.GetType().Name);

                    try
                    {
                        try
                        {
                            plugin.Init(logger, opsLogger, pluginConfiguration, moduleFolder, hostConfiguration.TransactionScopeTimeout);
                            pluginResults.Add(plugin.Context.Result);

                            plugin.BeforeExecute();
                            plugin.Execute();
                            plugin.AfterExecute();

                            AppendGlobalStat(globalStat, plugin.Context.Stat);

                            if (plugin.Context.Result.TerminateHost)
                            {
                                logger.Write(LogEventLevel.Error, "plugin requested to terminate the execution");
                                ExecutionTerminated = true;

                                startedOn.Stop();
                                runTimes.Add(startedOn.Elapsed);
                                break; // stop processing plugins
                            }

                            if (!plugin.Context.Result.Success)
                            {
                                AtLeastOnePluginFailed = true;
                            }
                        }
                        catch (Exception ex)
                        {
                            ExecutionTerminated = true;
                            logger.Write(LogEventLevel.Error, ex, "unhandled error during execution after {Elapsed}", startedOn.Elapsed);
                            opsLogger.Write(LogEventLevel.Error, "unhandled error during execution after {Elapsed}: {Message}", startedOn.Elapsed, ex.Message);

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

                    logger.Write(LogEventLevel.Information, "plugin execution finished in {Elapsed}", startedOn.Elapsed);
                }

                LogStats(globalStat, logger);

                for (var i = 0; i < Math.Min(modulePlugins.Count, pluginResults.Count); i++)
                {
                    var plugin = modulePlugins[i];
                    var result = pluginResults[i];
                    if (result.Success)
                    {
                        logger.Write(LogEventLevel.Information, "run-time of {PluginName} is {Elapsed}, status is {Status}", plugin.GetType().Name, runTimes[i], "success");
                    }
                    else
                    {
                        logger.Write(LogEventLevel.Information, "run-time of {PluginName} is {Elapsed}, status is {Status}, requested to terminate execution: {TerminateHost}", plugin.GetType().Name, runTimes[i], "failed", result.TerminateHost);
                    }
                }
            }
            catch (TransactionAbortedException)
            {
            }
        }

        private static void AppendGlobalStat(StatCounterCollection globalStat, StatCounterCollection stat)
        {
            foreach (var kvp in stat.GetCountersOrdered())
            {
                globalStat.IncrementCounter(kvp.Key, kvp.Value);
            }
        }

        private void LogStats(StatCounterCollection stats, ILogger logger)
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