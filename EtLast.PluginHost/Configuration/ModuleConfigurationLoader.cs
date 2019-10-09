namespace FizzCode.EtLast.PluginHost
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using FizzCode.DbTools.Configuration;
    using Microsoft.Extensions.Configuration;
    using Serilog.Events;

    internal static class ModuleConfigurationLoader
    {
        internal static ModuleConfiguration LoadModuleConfiguration(CommandContext commandContext, string moduleName, string[] moduleSettingOverrides, string[] pluginListOverride)
        {
            var sharedFolder = Path.Combine(commandContext.HostConfiguration.ModulesFolder, "Shared");
            var sharedConfigFileName = Path.Combine(sharedFolder, "shared-configuration.json");

            var moduleFolder = Path.Combine(commandContext.HostConfiguration.ModulesFolder, moduleName);
            if (!Directory.Exists(moduleFolder))
            {
                commandContext.Logger.Error("can't find the module folder: {ModuleFolder}", moduleFolder);
                commandContext.OpsLogger.Error("can't find the module folder: {ModuleFolder}", moduleFolder);
                return null;
            }

            moduleFolder = Directory.GetDirectories(commandContext.HostConfiguration.ModulesFolder, moduleName, SearchOption.TopDirectoryOnly).FirstOrDefault();
            moduleName = Path.GetFileName(moduleFolder);

            var configurationBuilder = new ConfigurationBuilder();

            if (File.Exists(sharedConfigFileName))
            {
                configurationBuilder.AddJsonFile(sharedConfigFileName);
                commandContext.Logger.Debug("using shared configuration file from {ConfigurationFilePath}", PathHelpers.GetFriendlyPathName(sharedConfigFileName));
            }

            var moduleConfigFileName = Path.Combine(moduleFolder, "module-configuration.json");
            if (!File.Exists(moduleConfigFileName))
            {
                commandContext.Logger.Error("can't find the module configuration file: {ConfigurationFilePath}", moduleConfigFileName);
                commandContext.OpsLogger.Error("can't find the module configuration file: {ConfigurationFilePath}", moduleConfigFileName);
                return null;
            }

            configurationBuilder.AddJsonFile(moduleConfigFileName);
            commandContext.Logger.Debug("using module configuration file from {ConfigurationFilePath}", PathHelpers.GetFriendlyPathName(moduleConfigFileName));

            var configuration = configurationBuilder.Build();
            AddCommandLineArgumentsToModuleConfiguration(configuration, moduleSettingOverrides);

            var pluginNamesToExecute = pluginListOverride;
            if (pluginNamesToExecute == null || pluginNamesToExecute.Length == 0)
                pluginNamesToExecute = configuration.GetSection("Module:PluginsToExecute-" + Environment.MachineName).Get<string[]>();
            if (pluginNamesToExecute == null || pluginNamesToExecute.Length == 0)
                pluginNamesToExecute = configuration.GetSection("Module:PluginsToExecute").Get<string[]>();

            var broken = false;
            foreach (var pluginName in pluginNamesToExecute.Where(x => x.Contains(',', StringComparison.InvariantCultureIgnoreCase) || x.Contains(' ', StringComparison.InvariantCultureIgnoreCase)))
            {
                commandContext.Logger.Error("plugin name can't contain comma or space character: [{Plugin}]", pluginName);
                broken = true;
            }
            if (broken)
                return null;

            var allConnectionStrings = new List<ConnectionStringWithProvider>();
            var sharedCs = new ConnectionStringCollection();
            sharedCs.LoadFromConfiguration(configuration, "ConnectionStrings:Shared");
            foreach (var cs in sharedCs.All)
            {
                allConnectionStrings.Add(cs);
            }

            var moduleCs = new ConnectionStringCollection();
            moduleCs.LoadFromConfiguration(configuration, "ConnectionStrings:Module");
            foreach (var cs in moduleCs.All)
            {
                allConnectionStrings.RemoveAll(x => x.Name == cs.Name);
                allConnectionStrings.Add(cs);
            }

            var originalNames = allConnectionStrings
                .Select(x => x.Name.Split('-')[0])
                .Distinct()
                .ToList();

            commandContext.Logger.Debug("connection strings for: {Module}", moduleName);
            var relevantCs = new ConnectionStringCollection();
            foreach (var originalName in originalNames)
            {
                var cs = allConnectionStrings.Find(x => string.Equals(x.Name, originalName + "-" + Environment.MachineName, StringComparison.InvariantCultureIgnoreCase))
                    ?? allConnectionStrings.Find(x => string.Equals(x.Name, originalName, StringComparison.InvariantCultureIgnoreCase));

                relevantCs.Add(cs);
                commandContext.Logger.Debug("\t{Name}, {ProviderName}, {ConnectionString}", cs.Name, cs.ProviderName, cs.ConnectionString);
            }

            return new ModuleConfiguration()
            {
                ModuleName = moduleName,
                ConfigurationFileName = moduleConfigFileName,
                ModuleFolder = moduleFolder,
                Configuration = configuration,
                ConnectionStrings = relevantCs,
                EnabledPluginList = pluginNamesToExecute
                    .Select(name => name.Trim())
                    .Where(name => !name.StartsWith("!", StringComparison.InvariantCultureIgnoreCase))
                    .Where(plugin => plugin != null)
                    .ToList(),
            };
        }

        private static void AddCommandLineArgumentsToModuleConfiguration(IConfigurationRoot configuration, string[] moduleSettingOverrides)
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
                    configuration["Module:" + key] = "true";
                }
                else
                {
                    var key = arg.Substring(0, idx).Trim();

                    configuration["Module:" + key] = arg.Substring(idx + 1).Trim();
                }
            }
        }
    }
}