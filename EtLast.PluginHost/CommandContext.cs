namespace FizzCode.EtLast.PluginHost
{
    using System;
    using System.IO;
    using System.Reflection;
    using Microsoft.Extensions.Configuration;
    using Serilog;

    public class CommandContext
    {
        public ILogger Logger { get; private set; }
        public ILogger OpsLogger { get; private set; }
        public ILogger IoLogger { get; private set; }
        public HostConfiguration HostConfiguration { get; private set; }

        public string LoadedConfigurationFileName { get; private set; }

        public bool Load()
        {
            LoadedConfigurationFileName = "host-configuration.json";
            LoadedConfigurationFileName = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), LoadedConfigurationFileName);

            if (!File.Exists(LoadedConfigurationFileName))
            {
                Logger = SerilogConfigurator.CreateLogger(null);
                OpsLogger = SerilogConfigurator.CreateOpsLogger(null);
                IoLogger = SerilogConfigurator.CreateIoLogger(null);

                Logger.Error("can't find the host configuration file: {FileName}", LoadedConfigurationFileName);
                OpsLogger.Error("can't find the host configuration file: {FileName}", LoadedConfigurationFileName);
                return false;
            }

            HostConfiguration = new HostConfiguration();

            try
            {
                var configuration = new ConfigurationBuilder()
                    .AddJsonFile(LoadedConfigurationFileName, false)
                    .Build();

                HostConfiguration.LoadFromConfiguration(configuration, "EtlHost");
            }
            catch (Exception ex)
            {
                throw new ConfigurationFileException(PathHelpers.GetFriendlyPathName(LoadedConfigurationFileName), "can't read the configuration file", ex);
            }

            if (HostConfiguration.ModulesFolder.StartsWith(@".\", StringComparison.InvariantCultureIgnoreCase))
            {
                HostConfiguration.ModulesFolder = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), HostConfiguration.ModulesFolder.Substring(2));
            }

            if (!Directory.Exists(HostConfiguration.ModulesFolder))
            {
                Logger.Error("can't find the specified modules folder: {ModulesFolder}", HostConfiguration.ModulesFolder);
                OpsLogger.Error("can't find the specified modules folder: {ModulesFolder}", HostConfiguration.ModulesFolder);
                return false;
            }

            Logger = SerilogConfigurator.CreateLogger(HostConfiguration);
            OpsLogger = SerilogConfigurator.CreateOpsLogger(HostConfiguration);
            IoLogger = SerilogConfigurator.CreateIoLogger(HostConfiguration);
            return true;
        }
    }
}