namespace FizzCode.EtLast.PluginHost
{
    using System;
    using Microsoft.Extensions.Configuration;
    using Serilog.Events;

    public class HostConfiguration
    {
        public TimeSpan TransactionScopeTimeout { get; set; } = TimeSpan.FromMinutes(120);
        public string SeqUrl { get; set; }
        public string SeqApiKey { get; set; }
        public int RetainedLogFileCountLimitHigh { get; set; } = 30;
        public int RetainedLogFileCountLimitInfo { get; set; } = 14;
        public int RetainedLogFileCountLimitDebug { get; set; } = 1;
        public int RetainedLogFileCountLimitVerbose { get; set; } = 1;
        public string ModulesFolder { get; set; } = @".\modules";
        public LogEventLevel MinimumLogLevelOnConsole { get; set; }
        public LogEventLevel MinimumLogLevelInFile { get; set; }
        public bool EnableDynamicCompilation { get; set; } = true;
        public bool ForceDynamicCompilation { get; set; } = true;

        public void LoadFromConfiguration(IConfigurationRoot configuration, string section)
        {
            SeqUrl = GetHostSetting<string>(configuration, section, "Seq:Url", null);
            SeqApiKey = GetHostSetting<string>(configuration, section, "Seq:ApiKey", null);
            RetainedLogFileCountLimitHigh = GetHostSetting(configuration, section, "RetainedLogFileCountLimit:Info", 30);
            RetainedLogFileCountLimitInfo = GetHostSetting(configuration, section, "RetainedLogFileCountLimit:Info", 14);
            RetainedLogFileCountLimitDebug = GetHostSetting(configuration, section, "RetainedLogFileCountLimit:Debug", 1);
            RetainedLogFileCountLimitVerbose = GetHostSetting(configuration, section, "RetainedLogFileCountLimit:Verbose", 1);
            TransactionScopeTimeout = TimeSpan.FromMinutes(GetHostSetting(configuration, section, "TransactionScopeTimeoutMinutes", 120));
            ModulesFolder = GetHostSetting(configuration, section, "ModulesFolder", @".\modules");
            EnableDynamicCompilation = GetHostSetting(configuration, section, "EnableDynamicCompilation", true);
            ForceDynamicCompilation = GetHostSetting(configuration, section, "ForceDynamicCompilation", false);

            var v = GetHostSetting<string>(configuration, section, "MinimumLogLevelOnConsole", null);
            if (!string.IsNullOrEmpty(v) && Enum.TryParse(v, out LogEventLevel level))
            {
                MinimumLogLevelOnConsole = level;
            }

            v = GetHostSetting<string>(configuration, section, "MinimumLogLevelInFile", null);
            if (!string.IsNullOrEmpty(v) && Enum.TryParse(v, out level))
            {
                MinimumLogLevelInFile = level;
            }
        }

        private static T GetHostSetting<T>(IConfigurationRoot configuration, string section, string key, T defaultValue)
        {
            var v = configuration.GetValue<T>(section + ":" + key + "-" + Environment.MachineName, default);
            if (v != null && !v.Equals(default(T)))
                return v;

            return configuration.GetValue(section + ":" + key, defaultValue);
        }
    }
}