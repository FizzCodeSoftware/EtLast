namespace FizzCode.EtLast.PluginHost
{
    using System;
    using Microsoft.Extensions.Configuration;
    using Serilog.Events;

    public class PluginHostConfiguration
    {
        public TimeSpan TransactionScopeTimeout { get; set; } = TimeSpan.FromMinutes(120);
        public string SeqUrl { get; set; }
        public string SeqApiKey { get; set; }
        public int RetainedLogFileCountLimit { get; set; } = 14;
        public string ModulesFolder { get; set; } = @".\modules";
        public LogEventLevel MinimumLogLevelOnConsole { get; set; }
        public bool EnableDynamicCompilation { get; set; } = true;
        public bool ForceDynamicCompilation { get; set; } = true;

        public void LoadFromConfiguration(IConfigurationRoot configuration, string section)
        {
            SeqUrl = GetHostSetting(configuration, section, "seq:Url", "-");
            SeqApiKey = GetHostSetting<string>(configuration, section, "seq:ApiKey", null);
            RetainedLogFileCountLimit = GetHostSetting(configuration, section, "RetainedLogFileCountLimit", 14);
            TransactionScopeTimeout = TimeSpan.FromMinutes(GetHostSetting(configuration, section, "TransactionScopeTimeoutMinutes", 120));
            ModulesFolder = GetHostSetting(configuration, section, "ModulesFolder", @".\modules");
            EnableDynamicCompilation = GetHostSetting(configuration, section, "EnableDynamicCompilation", true);
            ForceDynamicCompilation = GetHostSetting(configuration, section, "ForceDynamicCompilation", false);

            var v = GetHostSetting<string>(configuration, section, "MinimumLogLevelOnConsole", null);
            if (!string.IsNullOrEmpty(v) && Enum.TryParse(v, out LogEventLevel level))
            {
                MinimumLogLevelOnConsole = level;
            }
        }

        private static T GetHostSetting<T>(IConfigurationRoot configuration, string section, string key, T defaultValue)
        {
            var v = configuration.GetValue<T>(section + ":" + key + "-" + Environment.MachineName, default);
            if (v != null && !v.Equals(default(T)))
                return v;

            v = configuration.GetValue(section + ":" + key, defaultValue);
            return v ?? defaultValue;
        }
    }
}