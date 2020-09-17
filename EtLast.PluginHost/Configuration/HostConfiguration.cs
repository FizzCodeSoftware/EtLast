namespace FizzCode.EtLast.PluginHost
{
    using System;
    using System.Collections.Generic;
    using FizzCode.LightWeight.Configuration;
    using Microsoft.Extensions.Configuration;
    using Serilog.Events;

    public enum DynamicCompilationMode { Never, Always, Default }

    public class HostConfiguration
    {
        public TimeSpan TransactionScopeTimeout { get; set; } = TimeSpan.FromMinutes(120);

        public string SeqUrl { get; set; }
        public string SeqApiKey { get; set; }
        public int RetainedLogFileCountLimitImportant { get; set; } = 30;
        public int RetainedLogFileCountLimitInfo { get; set; } = 14;
        public int RetainedLogFileCountLimitLow { get; set; } = 4;
        public string ModulesFolder { get; set; } = @".\modules";
        public LogEventLevel MinimumLogLevelOnConsole { get; set; }
        public LogEventLevel MinimumLogLevelInFile { get; set; }
        public LogEventLevel MinimumLogLevelIo { get; set; }
        public DynamicCompilationMode DynamicCompilationMode { get; set; }
        public Dictionary<string, string> CommandAliases { get; set; } = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        public IConfigurationSecretProtector SecretProtector { get; set; }

        private IConfigurationRoot _configuration;
        private string _section;

        public void LoadFromConfiguration(IConfigurationRoot configuration, string section)
        {
            _configuration = configuration;
            _section = section;

            string v;

            if (ConfigurationReader.GetCurrentValue(configuration, section, "SecretProtector:Enabled", false))
            {
                v = ConfigurationReader.GetCurrentValue(configuration, section, "SecretProtector:Type", null);
                if (!string.IsNullOrEmpty(v))
                {
                    var type = Type.GetType(v);
                    if (type != null && typeof(IConfigurationSecretProtector).IsAssignableFrom(type))
                    {
                        var secretProtectorSection = configuration.GetSection(section + ":SecretProtector");
                        try
                        {
                            SecretProtector = (IConfigurationSecretProtector)Activator.CreateInstance(type);
                            if (!SecretProtector.Init(secretProtectorSection))
                            {
                                SecretProtector = null;
                            }
                        }
                        catch (Exception ex)
                        {
                            var exception = new Exception("Can't initialize secret protector.", ex);
                            exception.Data.Add("FullyQualifiedTypeName", v);
                            throw exception;
                        }
                    }
                    else
                    {
                        var exception = new Exception("Secret protector type not found.");
                        exception.Data.Add("FullyQualifiedTypeName", v);
                        throw exception;
                    }
                }
            }

            if (ConfigurationReader.GetCurrentValue(configuration, section, "Seq:Enabled", false))
            {
                SeqUrl = ConfigurationReader.GetCurrentValue(configuration, section, "Seq:Url", null, SecretProtector);
                SeqApiKey = ConfigurationReader.GetCurrentValue(configuration, section, "Seq:ApiKey", null, SecretProtector);
            }

            RetainedLogFileCountLimitImportant = ConfigurationReader.GetCurrentValue(configuration, section, "RetainedLogFileCountLimit:Important", 30);
            RetainedLogFileCountLimitInfo = ConfigurationReader.GetCurrentValue(configuration, section, "RetainedLogFileCountLimit:Info", 14);
            RetainedLogFileCountLimitLow = ConfigurationReader.GetCurrentValue(configuration, section, "RetainedLogFileCountLimit:Low", 4);
            TransactionScopeTimeout = TimeSpan.FromMinutes(ConfigurationReader.GetCurrentValue(configuration, section, "TransactionScopeTimeoutMinutes", 120));
            ModulesFolder = ConfigurationReader.GetCurrentValue(configuration, section, "ModulesFolder", @".\modules", SecretProtector);

            v = ConfigurationReader.GetCurrentValue(configuration, section, "DynamicCompilation:Mode", "Default", SecretProtector);
            if (!string.IsNullOrEmpty(v) && Enum.TryParse(v, out DynamicCompilationMode mode))
            {
                DynamicCompilationMode = mode;
            }

            v = ConfigurationReader.GetCurrentValue(configuration, section, "MinimumLogLevel:Console", "Information", SecretProtector);
            if (!string.IsNullOrEmpty(v) && Enum.TryParse(v, out LogEventLevel level))
            {
                MinimumLogLevelOnConsole = level;
            }

            v = ConfigurationReader.GetCurrentValue(configuration, section, "MinimumLogLevel:File", "Debug", SecretProtector);
            if (!string.IsNullOrEmpty(v) && Enum.TryParse(v, out level))
            {
                MinimumLogLevelInFile = level;
            }

            v = ConfigurationReader.GetCurrentValue(configuration, section, "MinimumLogLevel:IoFile", "Verbose", SecretProtector);
            if (!string.IsNullOrEmpty(v) && Enum.TryParse(v, out level))
            {
                MinimumLogLevelIo = level;
            }

            GetCommandAliases(configuration, section);
        }

        public List<IExecutionContextListener> GetExecutionContextListeners(IExecutionContext executionContext)
        {
            var result = new List<IExecutionContextListener>();

            var listenersSection = _configuration.GetSection(_section + ":ExecutionContextListeners");
            if (listenersSection == null)
                return result;

            var children = listenersSection.GetChildren();
            foreach (var childSection in children)
            {
                if (!ConfigurationReader.GetCurrentValue(childSection, "Enabled", false))
                    continue;

                var type = Type.GetType(childSection.Key);
                if (type != null && typeof(IExecutionContextListener).IsAssignableFrom(type))
                {
                    try
                    {
                        var instance = (IExecutionContextListener)Activator.CreateInstance(type);
                        var ok = instance.Init(executionContext, childSection, SecretProtector);
                        if (ok)
                        {
                            result.Add(instance);
                        }
                    }
                    catch (Exception ex)
                    {
                        var exception = new Exception("Can't initialize execution context listener.", ex);
                        exception.Data.Add("FullyQualifiedTypeName", childSection.Key);
                        throw exception;
                    }
                }
                else
                {
                    var exception = new Exception("ExecutionContextListener type not found.");
                    exception.Data.Add("FullyQualifiedTypeName", childSection.Key);
                    throw exception;
                }
            }

            return result;
        }

        private void GetCommandAliases(IConfigurationRoot configuration, string section)
        {
            var aliasSection = configuration.GetSection(section + ":Aliases");
            if (aliasSection == null)
                return;

            foreach (var child in aliasSection.GetChildren())
            {
                CommandAliases.Add(child.Key, child.Value);
            }
        }
    }
}