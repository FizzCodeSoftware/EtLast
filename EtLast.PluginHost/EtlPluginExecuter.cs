namespace FizzCode.EtLast.PluginHost
{
    using System;
    using System.Configuration;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using System.Xml.Linq;
    using Serilog;
    using Serilog.Events;

    public class EtlPluginExecuter
    {
        private ILogger _logger;
        private ILogger _opsLogger;
        private PluginHostConfiguration _configuration;

        public int Execute(PluginHostConfiguration configuration)
        {
            _configuration = configuration;

            var exitCode = Run();
            return exitCode;
        }

        public void AddExitCodeToEventLog(int exitCode)
        {
            using (var eventLog = new EventLog("Application"))
            {
                eventLog.Source = _configuration.CommandLineArguments.Length > 0
                    ? "EtlPluginExecuter+" + _configuration.CommandLineArguments[0].ToLowerInvariant()
                    : "EtlPluginExecuter";

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
                    case ExitCodes.ERR_AT_LEAST_ONE_PLUGIN_SCOPE_FAILED:
                    case ExitCodes.ERR_GLOBAL_SCOPE_FAILED:
                        eventLog.WriteEntry("ETL Plugin Executer failed due to plugin errors", EventLogEntryType.Information, exitCode);
                        break;
                }
            }
        }

        public void FixRoslynCompilerLocationInConfigFile(string configFileName)
        {
            var appDir = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);

            if (!File.Exists(configFileName))
            {
                return;
            }

            var content = File.ReadAllText(configFileName);
            var doc = XDocument.Load(configFileName);
            var appSettingsNode = doc.Descendants("appSettings").FirstOrDefault();

            var element = appSettingsNode.Descendants("add").FirstOrDefault(x => string.Compare(x.Attribute("key").Value, "aspnet:RoslynCompilerLocation", StringComparison.InvariantCultureIgnoreCase) == 0);
            element.SetAttributeValue("value", Path.Combine(appDir, "roslyn"));
            doc.Save(configFileName);

            AppDomain.CurrentDomain.SetData("APP_CONFIG_FILE", configFileName);

            typeof(ConfigurationManager).GetField("s_initState", BindingFlags.NonPublic | BindingFlags.Static).SetValue(null, 0);
            typeof(ConfigurationManager).GetField("s_configSystem", BindingFlags.NonPublic | BindingFlags.Static).SetValue(null, null);
            typeof(ConfigurationManager).Assembly.GetTypes().First(x => x.FullName == "System.Configuration.ClientConfigPaths")
                .GetField("s_current", BindingFlags.NonPublic | BindingFlags.Static)
                .SetValue(null, null);

            if (ConfigurationManager.AppSettings["aspnet:RoslynCompilerLocation"] == null)
            {
                throw new Exception("applying roslyn compiler location fix failed");
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
                _logger = new SerilogConfigurator().CreateLogger(_configuration);
                _opsLogger = new SerilogConfigurator().CreateOpsLogger(_configuration);

                new TransactionScopeTimeoutHack().ApplyHack(_configuration.TransactionScopeTimeout);
                new EnableVirtualTerminalProcessingHack().ApplyHack();

                if (_configuration.CommandLineArguments.Length == 0)
                {
                    _logger.Write(LogEventLevel.Error, "plugin folder command line argument is not defined");
                    _opsLogger.Write(LogEventLevel.Error, "plugin folder command line argument is not defined");
                    return ExitCodes.ERR_WRONG_ARGUMENTS;
                }

                var pluginFolder = _configuration.PluginFolder;
                if (pluginFolder.StartsWith(@".\"))
                {
                    pluginFolder = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), pluginFolder.Substring(2));
                }

                if (!Directory.Exists(pluginFolder))
                {
                    _logger.Write(LogEventLevel.Error, "can't find the specified plugin folder: {PluginFolder}", pluginFolder);
                    _opsLogger.Write(LogEventLevel.Error, "can't find the specified plugin folder: {PluginFolder}", pluginFolder);
                    return ExitCodes.ERR_NOTHING_TO_EXECUTE;
                }

                pluginFolder = Path.Combine(pluginFolder, _configuration.CommandLineArguments[0]);

                var configFilePath = Path.Combine(pluginFolder, "plugin.config");
                Configuration config = null;
                if (!File.Exists(configFilePath))
                {
                    _logger.Write(LogEventLevel.Error, "can't find plugin configuration file: {ConfigurationFilePath}", configFilePath);
                    _opsLogger.Write(LogEventLevel.Error, "can't find plugin configuration file: {ConfigurationFilePath}", configFilePath);
                    return ExitCodes.ERR_NO_CONFIG;
                }

                _logger.Write(LogEventLevel.Information, "loading plugin configuration file from {ConfigurationFilePath}", configFilePath);
                var configFileMap = new ConfigurationFileMap(configFilePath);
                config = ConfigurationManager.OpenMappedMachineConfiguration(configFileMap);

                var globalScopeRequired = string.Compare(config.AppSettings.Settings["GlobalScopeRequired"].Value, "true", true) == 0;
                var pluginScopeRequired = string.Compare(config.AppSettings.Settings["PluginScopeRequired"].Value, "true", true) == 0;

                var plugins = new PluginLoader().LoadPlugins(_logger, _opsLogger, AppDomain.CurrentDomain, pluginFolder, _configuration.CommandLineArguments[0]);
                var pluginNamesToExecute = GetAppSetting(config, "PluginsToExecute");

                plugins = pluginNamesToExecute.Split(',')
                            .Select(name => plugins.Find(plugin => plugin.GetType().Name == name))
                            .Where(plugin => plugin != null)
                            .ToList();

                _logger.Write(LogEventLevel.Information, "{PluginCount} plugin(s) found: {PluginNames}", plugins.Count, plugins.Select(x => x.GetType().Name));
                if (plugins == null || plugins.Count == 0)
                {
                    return ExitCodes.ERR_NOTHING_TO_EXECUTE;
                }

                var executer = new PluginExecuter();
                executer.ExecutePlugins(_configuration, plugins, globalScopeRequired, pluginScopeRequired, _logger, _opsLogger, config, pluginFolder);

                if (executer.GlobalScopeFailed)
                    return ExitCodes.ERR_GLOBAL_SCOPE_FAILED;
                if (executer.AtLeastOnePluginFailed)
                    return ExitCodes.ERR_AT_LEAST_ONE_PLUGIN_SCOPE_FAILED;
            }
            finally
            {
                AppDomain.CurrentDomain.UnhandledException -= UnhandledExceptionHandler;
            }

            return ExitCodes.ERR_NO_ERROR;
        }

        private string GetAppSetting(Configuration config, string key)
        {
            return config.AppSettings.Settings[key + "-" + Environment.MachineName] != null ? config.AppSettings.Settings[key + "-" + Environment.MachineName].Value : config.AppSettings.Settings[key].Value;
        }
    }
}