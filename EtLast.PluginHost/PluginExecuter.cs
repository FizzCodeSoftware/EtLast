namespace FizzCode.EtLast.PluginHost
{
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Diagnostics;
    using System.Transactions;
    using FizzCode.EtLast;
    using Serilog;
    using Serilog.Events;

    internal class PluginExecuter
    {
        public bool ExecutionTerminated { get; private set; }
        public bool AtLeastOnePluginFailed { get; private set; }

        public void ExecutePlugins(PluginHostConfiguration hostConfiguration, List<IEtlPlugin> plugins, ILogger logger, ILogger opsLogger, Configuration pluginConfiguration, string pluginFolder)
        {
            try
            {
                var runTimes = new List<TimeSpan>();
                var pluginResults = new List<EtlPluginResult>();
                foreach (var plugin in plugins)
                {
                    var sw = Stopwatch.StartNew();
                    logger.Write(LogEventLevel.Information, "executing {PluginTypeName}", plugin.GetType().Name);

                    try
                    {
                        try
                        {
                            var result = new EtlPluginResult();
                            pluginResults.Add(result);
                            plugin.Init(logger, opsLogger, pluginConfiguration, result, pluginFolder, hostConfiguration.TransactionScopeTimeout);
                            plugin.Execute();

                            if (result.Exceptions.Count > 0)
                            {
                                logger.Write(LogEventLevel.Error, "{ExceptionCount} exceptions raised during execution after {Elapsed}", result.Exceptions.Count, sw.Elapsed);
                                opsLogger.Write(LogEventLevel.Error, "{ExceptionCount} exceptions raised during execution after {Elapsed}", result.Exceptions.Count, sw.Elapsed);

                                var index = 0;
                                foreach (var ex in result.Exceptions)
                                {
                                    logger.Write(LogEventLevel.Error, ex, "exception #{ExceptionIndex}", index++);

                                    var opsMsg = ex.Message;
                                    if (ex.Data.Contains(EtlException.OpsMessageDataKey) && (ex.Data[EtlException.OpsMessageDataKey] != null))
                                    {
                                        opsMsg = ex.Data[EtlException.OpsMessageDataKey].ToString();
                                    }

                                    opsLogger.Write(LogEventLevel.Error, "exception #{ExceptionIndex}: {Message}", index++, opsMsg);
                                }
                            }

                            if (result.TerminateHost)
                            {
                                logger.Write(LogEventLevel.Error, "plugin requested to terminate the execution");
                                ExecutionTerminated = true;

                                sw.Stop();
                                runTimes.Add(sw.Elapsed);
                                break; // stop processing plugins
                            }

                            if (!result.Success)
                            {
                                AtLeastOnePluginFailed = true;
                            }
                        }
                        catch (Exception ex)
                        {
                            ExecutionTerminated = true;
                            logger.Write(LogEventLevel.Error, ex, "unhandled error during execution after {Elapsed}", sw.Elapsed);
                            opsLogger.Write(LogEventLevel.Error, "unhandled error during execution after {Elapsed}: {Message}", sw.Elapsed, ex.Message);

                            sw.Stop();
                            runTimes.Add(sw.Elapsed);
                            break; // stop processing plugins
                        }
                    }
                    catch (TransactionAbortedException) { }

                    sw.Stop();
                    runTimes.Add(sw.Elapsed);

                    logger.Write(LogEventLevel.Information, "plugin execution finished in {Elapsed}", sw.Elapsed);
                }

                for (var i = 0; i < Math.Min(plugins.Count, pluginResults.Count); i++)
                {
                    var plugin = plugins[i];
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
            catch (TransactionAbortedException) { }
        }
    }
}