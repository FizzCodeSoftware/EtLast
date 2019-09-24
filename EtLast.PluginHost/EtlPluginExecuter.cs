namespace FizzCode.EtLast.PluginHost
{
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using Serilog;
    using Serilog.Events;

    public class EtlPluginExecuter
    {
        private ILogger _logger;
        private ILogger _opsLogger;
        private PluginHostConfiguration _hostConfiguration;

        public int Execute(PluginHostConfiguration configuration)
        {
            _hostConfiguration = configuration;

            var exitCode = Run();
            return exitCode;
        }

        public void AddExitCodeToEventLog(int exitCode)
        {
            var sourceName = _hostConfiguration.CommandLineArguments.Length > 0
                    ? "EtlPluginExecuter+" + _hostConfiguration.CommandLineArguments[0].ToLowerInvariant()
                    : "EtlPluginExecuter";

            if (!EventLog.SourceExists(sourceName))
                EventLog.CreateEventSource(sourceName, "Application");

            using (var eventLog = new EventLog("Application"))
            {
                eventLog.Source = sourceName;

                switch (exitCode)
                {
                    case ExitCodes.ERR_NO_ERROR:
                        eventLog.WriteEntry("ETL Plugin Executer successfully finished", EventLogEntryType.Information, exitCode);
                        break;
                    case ExitCodes.ERR_NOTHING_TO_EXECUTE:
                    case ExitCodes.ERR_NO_CONFIG:
                    case ExitCodes.ERR_WRONG_ARGUMENTS:
                        eventLog.WriteEntry("ETL Plugin Executer failed due to configuration errors", EventLogEntryType.Information, exitCode);
                        break;
                    case ExitCodes.ERR_AT_LEAST_ONE_PLUGIN_FAILED:
                    case ExitCodes.ERR_EXECUTION_TERMINATED:
                        eventLog.WriteEntry("ETL Plugin Executer failed due to plugin errors", EventLogEntryType.Information, exitCode);
                        break;
                }
            }
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

        private int Run()
        {
            AppDomain.CurrentDomain.UnhandledException += UnhandledExceptionHandler;
            try
            {
                _logger = SerilogConfigurator.CreateLogger(_hostConfiguration);
                _opsLogger = SerilogConfigurator.CreateOpsLogger(_hostConfiguration);

                EnableVirtualTerminalProcessingHack.ApplyHack();

                if (_hostConfiguration.CommandLineArguments.Length == 0)
                {
                    _logger.Write(LogEventLevel.Error, "modules name command line argument is not defined");
                    _opsLogger.Write(LogEventLevel.Error, "module name command line argument is not defined");
                    return ExitCodes.ERR_WRONG_ARGUMENTS;
                }

                var modulesFolder = _hostConfiguration.ModulesFolder;
                if (modulesFolder.StartsWith(@".\", StringComparison.InvariantCultureIgnoreCase))
                {
                    modulesFolder = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), modulesFolder.Substring(2));
                }

                if (!Directory.Exists(modulesFolder))
                {
                    _logger.Write(LogEventLevel.Error, "can't find the specified modules folder: {PluginFolder}", modulesFolder);
                    _opsLogger.Write(LogEventLevel.Error, "can't find the specified modules folder: {PluginFolder}", modulesFolder);
                    return ExitCodes.ERR_NOTHING_TO_EXECUTE;
                }

                var sharedFolder = Path.Combine(modulesFolder, "Shared");
                var moduleFolder = Path.Combine(modulesFolder, _hostConfiguration.CommandLineArguments[0]);

                var moduleConfigFilePath = Path.Combine(moduleFolder, "module.config");
                Configuration moduleConfiguration = null;
                if (!File.Exists(moduleConfigFilePath))
                {
                    _logger.Write(LogEventLevel.Error, "can't find plugin configuration file: {ConfigurationFilePath}", moduleConfigFilePath);
                    _opsLogger.Write(LogEventLevel.Error, "can't find plugin configuration file: {ConfigurationFilePath}", moduleConfigFilePath);
                    return ExitCodes.ERR_NO_CONFIG;
                }

                _logger.Write(LogEventLevel.Information, "loading module configuration file from {ConfigurationFilePath}", moduleConfigFilePath);
                var configFileMap = new ConfigurationFileMap(moduleConfigFilePath);
                moduleConfiguration = ConfigurationManager.OpenMappedMachineConfiguration(configFileMap);

                FillModuleConfigAppSettings(moduleConfiguration);

                var sharedConfigFilePath = Path.Combine(sharedFolder, "shared.config");
                if (File.Exists(sharedConfigFilePath))
                {
                    _logger.Write(LogEventLevel.Information, "loading shared configuration file from {ConfigurationFilePath}", sharedConfigFilePath);

                    var sharedConfigFileMap = new ConfigurationFileMap(sharedConfigFilePath);
                    var sharedConfiguration = ConfigurationManager.OpenMappedMachineConfiguration(sharedConfigFileMap);
                    FillModuleConfigFromSharedConfig(moduleConfiguration, sharedConfiguration);
                }

                var modulePlugins = ModuleLoader.LoadModule(_logger, _opsLogger, moduleFolder, sharedFolder, _hostConfiguration.EnableDynamicCompilation, _hostConfiguration.CommandLineArguments[0]);
                modulePlugins = FilterExecutablePlugins(moduleConfiguration, modulePlugins);

                _logger.Write(LogEventLevel.Information, "{PluginCount} plugin(s) found: {PluginNames}",
                    modulePlugins.Count, modulePlugins.Select(plugin => TypeHelpers.GetFriendlyTypeName(plugin.GetType())).ToArray());

                if (modulePlugins.Count == 0)
                {
                    return ExitCodes.ERR_NOTHING_TO_EXECUTE;
                }

                var executer = new ModuleExecuter();
                executer.ExecuteModule(_hostConfiguration, modulePlugins, _logger, _opsLogger, moduleConfiguration, moduleFolder);

                if (executer.ExecutionTerminated)
                    return ExitCodes.ERR_EXECUTION_TERMINATED;

                if (executer.AtLeastOnePluginFailed)
                    return ExitCodes.ERR_AT_LEAST_ONE_PLUGIN_FAILED;
            }
            finally
            {
                AppDomain.CurrentDomain.UnhandledException -= UnhandledExceptionHandler;
            }

            return ExitCodes.ERR_NO_ERROR;
        }

        private static List<IEtlPlugin> FilterExecutablePlugins(Configuration moduleConfiguration, List<IEtlPlugin> plugins)
        {
            if (plugins == null || plugins.Count == 0)
                return new List<IEtlPlugin>();

            var pluginNamesToExecute = GetAppSetting(moduleConfiguration, "PluginsToExecute");
            return pluginNamesToExecute.Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries)
                .Select(name => name.Trim())
                .Where(name => !name.StartsWith("!", StringComparison.InvariantCultureIgnoreCase))
                .Select(name => plugins.Find(plugin => plugin.GetType().Name == name))
                .Where(plugin => plugin != null)
                .ToList();
        }

        private void FillModuleConfigAppSettings(Configuration moduleConfiguration)
        {
            if (_hostConfiguration.CommandLineArguments.Length <= 1)
                return;

            for (var i = 1; i < _hostConfiguration.CommandLineArguments.Length; i++)
            {
                var arg = _hostConfiguration.CommandLineArguments[i].Trim();
                var idx = arg.IndexOf('=', StringComparison.InvariantCultureIgnoreCase);
                if (idx == -1)
                {
                    var key = arg;
                    if (moduleConfiguration.AppSettings.Settings[key] != null)
                        moduleConfiguration.AppSettings.Settings.Remove(key);

                    moduleConfiguration.AppSettings.Settings.Add(key, string.Empty);
                }
                else
                {
                    var key = arg.Substring(0, idx).Trim();
                    if (moduleConfiguration.AppSettings.Settings[key] != null)
                        moduleConfiguration.AppSettings.Settings.Remove(key);

                    moduleConfiguration.AppSettings.Settings.Add(key, arg.Substring(idx + 1).Trim());
                }
            }
        }

        private static void FillModuleConfigFromSharedConfig(Configuration moduleConfiguration, Configuration sharedConfiguration)
        {
            foreach (var key in sharedConfiguration.AppSettings.Settings.AllKeys)
            {
                if (moduleConfiguration.AppSettings.Settings[key] != null)
                    continue;

                var value = sharedConfiguration.AppSettings.Settings[key].Value;
                moduleConfiguration.AppSettings.Settings.Add(key, value);
            }

            foreach (ConnectionStringSettings connectionStringSettings in sharedConfiguration.ConnectionStrings.ConnectionStrings)
            {
                if (moduleConfiguration.ConnectionStrings.ConnectionStrings[connectionStringSettings.Name] != null)
                    continue;

                var newSettings = new ConnectionStringSettings(connectionStringSettings.Name, connectionStringSettings.ConnectionString, connectionStringSettings.ProviderName);
                moduleConfiguration.ConnectionStrings.ConnectionStrings.Add(newSettings);
            }
        }

        private static string GetAppSetting(Configuration config, string key)
        {
            return config.AppSettings.Settings[key + "-" + Environment.MachineName] != null
                ? config.AppSettings.Settings[key + "-" + Environment.MachineName].Value
                : config.AppSettings.Settings[key].Value;
        }
    }
}