namespace FizzCode.EtLast.PluginHost
{
    using System;
    using System.Configuration;
    using Serilog.Events;

    public class PluginHostConfiguration
    {
        public string[] CommandLineArguments { get; set; }
        public TimeSpan TransactionScopeTimeout { get; set; } = TimeSpan.FromMinutes(120);
        public string SeqUrl { get; set; }
        public string SeqApiKey { get; set; }
        public int RetainedLogFileCountLimit { get; set; } = 14;
        public string ModulesFolder { get; set; } = @".\modules";
        public LogEventLevel MinimumLogLevelOnConsole { get; set; }

        public void LoadFromStandardAppSettings()
        {
            SeqUrl = GetAppSetting("seq:Url");
            SeqApiKey = GetAppSetting("seq:ApiKey");
            RetainedLogFileCountLimit = GetAppSettingAsInt("RetainedLogFileCountLimit", 14);
            TransactionScopeTimeout = TimeSpan.FromMinutes(GetAppSettingAsInt("TransactionScopeTimeoutMinutes", 120));
            ModulesFolder = GetAppSetting("ModulesFolder");

            var v = GetAppSetting("MinimumLogLevelOnConsole");
            if (!string.IsNullOrEmpty(v))
            {
                if (Enum.TryParse(v, out LogEventLevel level))
                {
                    MinimumLogLevelOnConsole = level;
                }
            }
        }

        public static int GetAppSettingAsInt(string key, int defaultValue)
        {
            var value = GetAppSetting(key);
            return string.IsNullOrEmpty(value)
                ? defaultValue
                : int.TryParse(value, out var iv)
                    ? iv
                    : defaultValue;
        }

        public static string GetAppSetting(string key)
        {
            return ConfigurationManager.AppSettings[key + "-" + Environment.MachineName] ?? ConfigurationManager.AppSettings[key];
        }
    }
}