namespace FizzCode.EtLast.PluginHost
{
    using System;
    using System.IO;
    using Microsoft.Extensions.Configuration;
    using Serilog.Events;

    internal static class ModuleConfigurationLoader
    {
        internal static ModuleConfiguration LoadModuleConfiguration(CommandContext commandContext, string moduleName, string[] moduleSettingOverrides)
        {
            var sharedFolder = Path.Combine(commandContext.HostConfiguration.ModulesFolder, "Shared");
            var sharedConfigFileName = Path.Combine(sharedFolder, "shared-configuration.json");

            var moduleFolder = Path.Combine(commandContext.HostConfiguration.ModulesFolder, moduleName);
            if (!Directory.Exists(moduleFolder))
            {
                commandContext.Logger.Write(LogEventLevel.Error, "can't find the module folder: {ModuleFolder}", moduleFolder);
                commandContext.OpsLogger.Write(LogEventLevel.Error, "can't find the module folder: {ModuleFolder}", moduleFolder);
                return null;
            }

            var configurationBuilder = new ConfigurationBuilder();

            if (File.Exists(sharedConfigFileName))
            {
                configurationBuilder.AddJsonFile(sharedConfigFileName);
                commandContext.Logger.Write(LogEventLevel.Debug, "using shared configuration file from {ConfigurationFilePath}", PathHelpers.GetFriendlyPathName(sharedConfigFileName));
            }

            var moduleConfigFileName = Path.Combine(moduleFolder, "module-configuration.json");
            if (!File.Exists(moduleConfigFileName))
            {
                commandContext.Logger.Write(LogEventLevel.Error, "can't find the module configuration file: {ConfigurationFilePath}", moduleConfigFileName);
                commandContext.OpsLogger.Write(LogEventLevel.Error, "can't find the module configuration file: {ConfigurationFilePath}", moduleConfigFileName);
                return null;
            }

            configurationBuilder.AddJsonFile(moduleConfigFileName);
            commandContext.Logger.Write(LogEventLevel.Debug, "using module configuration file from {ConfigurationFilePath}", PathHelpers.GetFriendlyPathName(moduleConfigFileName));

            var moduleConfiguration = configurationBuilder.Build();

            AddCommandLineArgumentsToModuleConfiguration(moduleConfiguration, moduleSettingOverrides);

            return new ModuleConfiguration()
            {
                ModuleName = moduleName,
                ConfigurationFileName = moduleConfigFileName,
                ModuleFolder = moduleFolder,
                Configuration = moduleConfiguration,
            };
        }

        private static void AddCommandLineArgumentsToModuleConfiguration(IConfigurationRoot moduleConfiguration, string[] moduleSettingOverrides)
        {
            if (moduleSettingOverrides == null)
                return;

            for (var i = 0; i < moduleSettingOverrides.Length; i++)
            {
                var arg = moduleSettingOverrides[i].Trim();
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