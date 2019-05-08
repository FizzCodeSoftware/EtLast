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
        public bool GlobalScopeFailed { get; private set; }
        public bool AtLeastOnePluginFailed { get; private set; }

        public void ExecutePlugins(PluginHostConfiguration hostConfiguration, List<IEtlPlugin> plugins, bool globalScopeRequired, bool pluginScopeRequired, ILogger logger, ILogger opsLogger, Configuration pluginConfiguration, string pluginFolder)
        {
            try
            {
                using (var globalScope = globalScopeRequired ? new TransactionScope(TransactionScopeOption.Required, hostConfiguration.TransactionScopeTimeout) : null)
                {
                    var runTimes = new List<TimeSpan>();
                    var pluginResults = new List<EtlPluginResult>();
                    foreach (var plugin in plugins)
                    {
                        var sw = Stopwatch.StartNew();
                        logger.Write(LogEventLevel.Information, "executing {PluginTypeName}", plugin.GetType().Name);

                        try
                        {
                            using (var pluginScope = pluginScopeRequired ? new TransactionScope(TransactionScopeOption.Required, hostConfiguration.TransactionScopeTimeout) : null)
                            {
                                try
                                {
                                    var result = new EtlPluginResult();
                                    pluginResults.Add(result);
                                    plugin.Init(logger, opsLogger, pluginConfiguration, result, pluginFolder);
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

                                    if (result.TerminateGlobalScope)
                                    {
                                        logger.Write(LogEventLevel.Error, "plugin requested to terminate the global scope");
                                        GlobalScopeFailed = true;

                                        sw.Stop();
                                        runTimes.Add(sw.Elapsed);
                                        break; // stop processing plugins
                                    }

                                    if (!result.TerminatePluginScope)
                                    {
                                        if (pluginScopeRequired)
                                        {
                                            pluginScope.Complete();
                                        }
                                    }
                                    else
                                    {
                                        AtLeastOnePluginFailed = true;
                                        logger.Write(LogEventLevel.Error, "plugin requested to terminate the plugin scope");
                                    }
                                }
                                catch (Exception ex)
                                {
                                    GlobalScopeFailed = true;
                                    logger.Write(LogEventLevel.Error, ex, "unhandled error during execution after {Elapsed}", sw.Elapsed);
                                    opsLogger.Write(LogEventLevel.Error, "unhandled error during execution after {Elapsed}: {Message}", sw.Elapsed, ex.Message);

                                    sw.Stop();
                                    runTimes.Add(sw.Elapsed);
                                    break; // stop processing plugins
                                }
                            } // plugin scope disposed
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
                        if (!result.TerminateGlobalScope && !result.TerminatePluginScope)
                        {
                            logger.Write(LogEventLevel.Information, "run-time of {PluginName} is {Elapsed}, status is {Status}", plugin.GetType().Name, runTimes[i], "success");
                        }
                        else
                        {
                            logger.Write(LogEventLevel.Information, "run-time of {PluginName} is {Elapsed}, status is {Status}, requested to terminate global scope: {TerminateGlobalScope}, requested to terminate plugin scope: {TerminatePluginScope}", plugin.GetType().Name, runTimes[i], "failed", result.TerminateGlobalScope, result.TerminatePluginScope);
                        }
                    }

                    if (!GlobalScopeFailed)
                    {
                        if (globalScopeRequired)
                        {
                            globalScope.Complete();
                        }
                    }
                }
            }
            catch (TransactionAbortedException) { }
        }
    }
}