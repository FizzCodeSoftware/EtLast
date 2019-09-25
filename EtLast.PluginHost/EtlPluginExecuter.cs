namespace FizzCode.EtLast.PluginHost
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using Microsoft.Extensions.Configuration;
    using Serilog;
    using Serilog.Events;

    public class EtlPluginExecuter
    {
        private ILogger _logger;
        private ILogger _opsLogger;
        private PluginHostConfiguration _hostConfiguration;

        public ExitCode Execute(PluginHostConfiguration configuration)
        {
            _hostConfiguration = configuration;

            var exitCode = Run();
            return exitCode;
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

        private ExitCode Run()
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
                    return ExitCode.ERR_WRONG_ARGUMENTS;
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
                    return ExitCode.ERR_NOTHING_TO_EXECUTE;
                }

                var sharedFolder = Path.Combine(modulesFolder, "Shared");
                var moduleFolder = Path.Combine(modulesFolder, _hostConfiguration.CommandLineArguments[0]);

                var configurationBuilder = new ConfigurationBuilder();

                var sharedConfigFilePath = Path.Combine(sharedFolder, "shared-configuration.json");
                if (File.Exists(sharedConfigFilePath))
                {
                    configurationBuilder.AddJsonFile(sharedConfigFilePath);
                    _logger.Write(LogEventLevel.Information, "loading shared configuration file from {ConfigurationFilePath}", sharedConfigFilePath);
                }

                var moduleConfigFilePath = Path.Combine(moduleFolder, "module-configuration.json");
                if (!File.Exists(moduleConfigFilePath))
                {
                    _logger.Write(LogEventLevel.Error, "can't find the module's configuration file: {ConfigurationFilePath}", moduleConfigFilePath);
                    _opsLogger.Write(LogEventLevel.Error, "can't find the module's configuration file: {ConfigurationFilePath}", moduleConfigFilePath);
                    return ExitCode.ERR_NO_CONFIG;
                }

                configurationBuilder.AddJsonFile(moduleConfigFilePath);
                _logger.Write(LogEventLevel.Information, "loading module configuration file from {ConfigurationFilePath}", moduleConfigFilePath);

                var moduleConfiguration = configurationBuilder.Build();

                AddCommandLineArgumentsToModuleConfiguration(moduleConfiguration);

                var modulePlugins = ModuleLoader.LoadModule(_logger, _opsLogger, moduleFolder, sharedFolder, _hostConfiguration.EnableDynamicCompilation, _hostConfiguration.CommandLineArguments[0]);
                modulePlugins = FilterExecutablePlugins(moduleConfiguration, modulePlugins);

                _logger.Write(LogEventLevel.Information, "{PluginCount} plugin(s) found: {PluginNames}",
                    modulePlugins.Count, modulePlugins.Select(plugin => TypeHelpers.GetFriendlyTypeName(plugin.GetType())).ToArray());

                if (modulePlugins.Count == 0)
                {
                    return ExitCode.ERR_NOTHING_TO_EXECUTE;
                }

                var executer = new ModuleExecuter();
                executer.ExecuteModule(_hostConfiguration, modulePlugins, _logger, _opsLogger, moduleConfiguration, moduleFolder);

                if (executer.ExecutionTerminated)
                    return ExitCode.ERR_EXECUTION_TERMINATED;

                if (executer.AtLeastOnePluginFailed)
                    return ExitCode.ERR_AT_LEAST_ONE_PLUGIN_FAILED;
            }
            finally
            {
                AppDomain.CurrentDomain.UnhandledException -= UnhandledExceptionHandler;
            }

            return ExitCode.ERR_NO_ERROR;
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

        private void AddCommandLineArgumentsToModuleConfiguration(IConfigurationRoot moduleConfiguration)
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
                    moduleConfiguration["Module:" + key] = "true";
                }
                else
                {
                    var key = arg.Substring(0, idx).Trim();

                    moduleConfiguration["Module:" + key] = arg.Substring(idx + 1).Trim();
                }
            }
        }
    }
}