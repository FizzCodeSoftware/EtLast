namespace FizzCode.EtLast.PluginHost
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using System.Transactions;
    using Microsoft.Extensions.Configuration;
    using Serilog;
    using Serilog.Events;

    public class ModuleExecuter
    {
        private ILogger _logger;
        private ILogger _opsLogger;
        private PluginHostConfiguration _hostConfiguration;

        public ExecutionResult Execute(string[] commandLineArguments)
        {
            var hostConfigurationFileName = "host-configuration.json";
            if (!File.Exists(hostConfigurationFileName))
            {
                _logger = SerilogConfigurator.CreateLogger(null);
                _opsLogger = SerilogConfigurator.CreateOpsLogger(null);

                _logger.Write(LogEventLevel.Error, "can't find the host configuration file: {ConfigurationFilePath}", hostConfigurationFileName);
                _opsLogger.Write(LogEventLevel.Error, "can't find the host configuration file: {ConfigurationFilePath}", hostConfigurationFileName);

                return ExecutionResult.MissingConfigurationFile;
            }

            _hostConfiguration = new PluginHostConfiguration();

            var configuration = new ConfigurationBuilder()
                .AddJsonFile(hostConfigurationFileName, false)
                .Build();

            _hostConfiguration.LoadFromConfiguration(configuration, "PluginHost");

            _logger = SerilogConfigurator.CreateLogger(_hostConfiguration);
            _opsLogger = SerilogConfigurator.CreateOpsLogger(_hostConfiguration);

            if (commandLineArguments.Length == 0)
            {
                _logger.Write(LogEventLevel.Error, "modules name command line argument is not defined");
                _opsLogger.Write(LogEventLevel.Error, "module name command line argument is not defined");
                return ExecutionResult.WrongArguments;
            }

            var moduleNames = commandLineArguments[0].Split(',');

            _logger.Write(LogEventLevel.Information, "executing modules: {ModuleList}", moduleNames);

            var modulesFolder = _hostConfiguration.ModulesFolder;
            if (modulesFolder.StartsWith(@".\", StringComparison.InvariantCultureIgnoreCase))
            {
                modulesFolder = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), modulesFolder.Substring(2));
            }

            if (!Directory.Exists(modulesFolder))
            {
                _logger.Write(LogEventLevel.Error, "can't find the specified modules folder: {ModulesFolder}", modulesFolder);
                _opsLogger.Write(LogEventLevel.Error, "can't find the specified modules folder: {ModulesFolder}", modulesFolder);
                return ExecutionResult.WrongArguments;
            }

            var sharedFolder = Path.Combine(modulesFolder, "Shared");
            var sharedConfigFileName = Path.Combine(sharedFolder, "shared-configuration.json");

            foreach (var moduleName in moduleNames)
            {
                var moduleConfigFileName = Path.Combine(modulesFolder, moduleName, "module-configuration.json");
                if (!File.Exists(moduleConfigFileName))
                {
                    _logger.Write(LogEventLevel.Error, "can't find the module configuration file: {ConfigurationFilePath}", moduleConfigFileName);
                    _opsLogger.Write(LogEventLevel.Error, "can't find the module configuration file: {ConfigurationFilePath}", moduleConfigFileName);
                    return ExecutionResult.MissingConfigurationFile;
                }
            }

            AppDomain.CurrentDomain.UnhandledException += UnhandledExceptionHandler;
            try
            {
                foreach (var moduleName in moduleNames)
                {
                    var moduleFolder = Path.Combine(modulesFolder, moduleName);

                    var configurationBuilder = new ConfigurationBuilder();

                    if (File.Exists(sharedConfigFileName))
                    {
                        configurationBuilder.AddJsonFile(sharedConfigFileName);
                        _logger.Write(LogEventLevel.Information, "using shared configuration file from {ConfigurationFilePath}", PathHelpers.GetFriendlyPathName(sharedConfigFileName));
                    }

                    var moduleConfigFileName = Path.Combine(moduleFolder, "module-configuration.json");
                    configurationBuilder.AddJsonFile(moduleConfigFileName);
                    _logger.Write(LogEventLevel.Information, "using module configuration file from {ConfigurationFilePath}", PathHelpers.GetFriendlyPathName(moduleConfigFileName));

                    var moduleConfiguration = configurationBuilder.Build();

                    AddCommandLineArgumentsToModuleConfiguration(moduleConfiguration, commandLineArguments);

                    var modulePlugins = ModuleLoader.LoadModule(_logger, _opsLogger, moduleFolder, sharedFolder, _hostConfiguration.EnableDynamicCompilation, moduleName);
                    modulePlugins = FilterExecutablePlugins(moduleConfiguration, modulePlugins);

                    _logger.Write(LogEventLevel.Information, "{PluginCount} plugin(s) found: {PluginNames}",
                        modulePlugins.Count, modulePlugins.Select(plugin => TypeHelpers.GetFriendlyTypeName(plugin.GetType())).ToArray());

                    if (modulePlugins.Count > 0)
                    {
                        var moduleResult = ExecuteModule(modulePlugins, moduleConfiguration, moduleFolder);

                        if (moduleResult != ExecutionResult.Success)
                            return moduleResult;
                    }
                }
            }
            finally
            {
                AppDomain.CurrentDomain.UnhandledException -= UnhandledExceptionHandler;
            }

            return ExecutionResult.Success;
        }

        private void UnhandledExceptionHandler(object sender, UnhandledExceptionEventArgs e)
        {
            if (_logger != null)
            {
                _logger.Error(e.ExceptionObject as Exception, "unexpected error during execution");
                _opsLogger.Error("unexpected error during execution: {Message}", (e.ExceptionObject as Exception)?.Message);
            }
            else
            {
                Console.WriteLine("unexpected error during execution: " + e.ExceptionObject.ToString());
            }

            Environment.Exit(-1);
        }

        private static List<IEtlPlugin> FilterExecutablePlugins(IConfigurationRoot moduleConfiguration, List<IEtlPlugin> plugins)
        {
            if (plugins == null || plugins.Count == 0)
                return new List<IEtlPlugin>();

            var pluginNamesToExecute = moduleConfiguration.GetSection("Module:PluginsToExecute-" + Environment.MachineName).Get<string[]>();
            if (pluginNamesToExecute == null || pluginNamesToExecute.Length == 0)
                pluginNamesToExecute = moduleConfiguration.GetSection("Module:PluginsToExecute").Get<string[]>();

            return pluginNamesToExecute
                .Select(name => name.Trim())
                .Where(name => !name.StartsWith("!", StringComparison.InvariantCultureIgnoreCase))
                .Select(name => plugins.Find(plugin => plugin.GetType().Name == name))
                .Where(plugin => plugin != null)
                .ToList();
        }

        private static void AddCommandLineArgumentsToModuleConfiguration(IConfigurationRoot moduleConfiguration, string[] commandLineArguments)
        {
            if (commandLineArguments.Length <= 1)
                return;

            for (var i = 1; i < commandLineArguments.Length; i++)
            {
                var arg = commandLineArguments[i].Trim();
                var idx = arg.IndexOf('=', StringComparison.InvariantCultureIgnoreCase);
                if (idx == -1)
                {
                    var key = arg;
                    moduleConfiguration["Module:" + key] = "true";
                }
                else
                {
                    var key = arg.Substring(0, idx).Trim();

                    moduleConfiguration["Module:" + key] = arg.Substring(idx + 1).Trim();
                }
            }
        }

        private ExecutionResult ExecuteModule(List<IEtlPlugin> modulePlugins, IConfigurationRoot moduleConfiguration, string moduleFolder)
        {
            var result = ExecutionResult.Success;

            try
            {
                var globalStat = new StatCounterCollection();
                var runTimes = new List<TimeSpan>();
                var pluginResults = new List<EtlContextResult>();
                foreach (var plugin in modulePlugins)
                {
                    var startedOn = Stopwatch.StartNew();
                    _logger.Write(LogEventLevel.Information, "executing {PluginTypeName}", TypeHelpers.GetFriendlyTypeName(plugin.GetType()));

                    try
                    {
                        try
                        {
                            plugin.Init(_logger, _opsLogger, moduleConfiguration, moduleFolder, _hostConfiguration.TransactionScopeTimeout);
                            pluginResults.Add(plugin.Context.Result);

                            plugin.BeforeExecute();
                            plugin.Execute();
                            plugin.AfterExecute();

                            AppendGlobalStat(globalStat, plugin.Context.Stat);

                            if (plugin.Context.Result.TerminateHost)
                            {
                                _logger.Write(LogEventLevel.Error, "plugin requested to terminate the execution");
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
                            _logger.Write(LogEventLevel.Error, ex, "unhandled error during execution after {Elapsed}", startedOn.Elapsed);
                            _opsLogger.Write(LogEventLevel.Error, "unhandled error during execution after {Elapsed}: {Message}", startedOn.Elapsed, ex.Message);

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

                    _logger.Write(LogEventLevel.Information, "plugin execution finished in {Elapsed}", startedOn.Elapsed);
                }

                LogStats(globalStat, _logger);

                for (var i = 0; i < Math.Min(modulePlugins.Count, pluginResults.Count); i++)
                {
                    var plugin = modulePlugins[i];
                    var pluginResult = pluginResults[i];
                    if (pluginResult.Success)
                    {
                        _logger.Write(LogEventLevel.Information, "run-time of {PluginName} is {Elapsed}, status is {Status}", TypeHelpers.GetFriendlyTypeName(plugin.GetType()), runTimes[i], "success");
                    }
                    else
                    {
                        _logger.Write(LogEventLevel.Information, "run-time of {PluginName} is {Elapsed}, status is {Status}, requested to terminate execution: {TerminateHost}", TypeHelpers.GetFriendlyTypeName(plugin.GetType()), runTimes[i], "failed", pluginResult.TerminateHost);
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