namespace FizzCode.EtLast.PluginHost
{
    using System;
    using System.Configuration;
    using System.Diagnostics;
    using System.Globalization;
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
        private PluginHostConfiguration _hostConfiguration;

        public int Execute(PluginHostConfiguration configuration)
        {
            _hostConfiguration = configuration;

            var exitCode = Run();
            return exitCode;
        }

        public void AddExitCodeToEventLog(int exitCode)
        {
            using (var eventLog = new EventLog("Application"))
            {
                eventLog.Source = _hostConfiguration.CommandLineArguments.Length > 0
                    ? "EtlPluginExecuter+" + _hostConfiguration.CommandLineArguments[0].ToLowerInvariant()
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
                _logger = new SerilogConfigurator().CreateLogger(_hostConfiguration);
                _opsLogger = new SerilogConfigurator().CreateOpsLogger(_hostConfiguration);

                new TransactionScopeTimeoutHack().ApplyHack(_hostConfiguration.TransactionScopeTimeout);
                new EnableVirtualTerminalProcessingHack().ApplyHack();

                if (_hostConfiguration.CommandLineArguments.Length == 0)
                {
                    _logger.Write(LogEventLevel.Error, "plugin folder command line argument is not defined");
                    _opsLogger.Write(LogEventLevel.Error, "plugin folder command line argument is not defined");
                    return ExitCodes.ERR_WRONG_ARGUMENTS;
                }

                var pluginFolder = _hostConfiguration.PluginFolder;
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

                pluginFolder = Path.Combine(pluginFolder, _hostConfiguration.CommandLineArguments[0]);

                var pluginConfigFilePath = Path.Combine(pluginFolder, "plugin.config");
                Configuration pluginConfiguration = null;
                if (!File.Exists(pluginConfigFilePath))
                {
                    _logger.Write(LogEventLevel.Error, "can't find plugin configuration file: {ConfigurationFilePath}", pluginConfigFilePath);
                    _opsLogger.Write(LogEventLevel.Error, "can't find plugin configuration file: {ConfigurationFilePath}", pluginConfigFilePath);
                    return ExitCodes.ERR_NO_CONFIG;
                }

                _logger.Write(LogEventLevel.Information, "loading plugin configuration file from {ConfigurationFilePath}", pluginConfigFilePath);
                var configFileMap = new ConfigurationFileMap(pluginConfigFilePath);
                pluginConfiguration = ConfigurationManager.OpenMappedMachineConfiguration(configFileMap);

                FullPluginConfigAppSettings(pluginConfiguration);

                var globalScopeRequired = string.Compare(pluginConfiguration.AppSettings.Settings["GlobalScopeRequired"].Value, "true", true) == 0;
                var pluginScopeRequired = string.Compare(pluginConfiguration.AppSettings.Settings["PluginScopeRequired"].Value, "true", true) == 0;

                var plugins = new PluginLoader().LoadPlugins(_logger, _opsLogger, AppDomain.CurrentDomain, pluginFolder, _hostConfiguration.CommandLineArguments[0]);
                var pluginNamesToExecute = GetAppSetting(pluginConfiguration, "PluginsToExecute");

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
                executer.ExecutePlugins(_hostConfiguration, plugins, globalScopeRequired, pluginScopeRequired, _logger, _opsLogger, pluginConfiguration, pluginFolder);

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

        private void FullPluginConfigAppSettings(Configuration pluginConfiguration)
        {
            if (_hostConfiguration.CommandLineArguments.Length > 1)
            {
                for (var i = 1; i < _hostConfiguration.CommandLineArguments.Length; i++)
                {
                    var arg = _hostConfiguration.CommandLineArguments[i].Trim();
                    var idx = arg.IndexOf('=');
                    if (idx == -1)
                    {
                        if (pluginConfiguration.AppSettings.Settings[arg] == null)
                        {
                            pluginConfiguration.AppSettings.Settings.Add(arg, i.ToString("D", CultureInfo.InvariantCulture));
                        }
                    }
                    else
                    {
                        var key = arg.Substring(0, idx).Trim();
                        if (pluginConfiguration.AppSettings.Settings[key] == null)
                        {
                            pluginConfiguration.AppSettings.Settings.Add(key, arg.Substring(idx + 1).Trim());
                        }
                    }
                }
            }
        }

        private string GetAppSetting(Configuration config, string key)
        {
            return config.AppSettings.Settings[key + "-" + Environment.MachineName] != null
                ? config.AppSettings.Settings[key + "-" + Environment.MachineName].Value
                : config.AppSettings.Settings[key].Value;
        }
    }
}