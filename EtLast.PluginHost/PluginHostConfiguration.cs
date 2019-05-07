﻿namespace FizzCode.EtLast.PluginHost
{
    using Serilog.Events;
    using System;
    using System.Configuration;

    public class PluginHostConfiguration
    {
        public string[] CommandLineArguments { get; set; }
        public TimeSpan TransactionScopeTimeout { get; set; } = TimeSpan.FromMinutes(120);
        public string SeqUrl { get; set; } = null;
        public string SeqApiKey { get; set; } = null;
        public int RetainedLogFileCountLimit { get; set; } = 14;
        public string PluginFolder { get; set; } = @".\plugins";
        public LogEventLevel MinimumLogLevelOnConsole { get; set; } = LogEventLevel.Verbose;

        public void LoadFromStandardAppSettings()
        {
            SeqUrl = GetAppSetting("seq:Url");
            SeqApiKey = GetAppSetting("seq:ApiKey");
            RetainedLogFileCountLimit = GetAppSettingAsInt("RetainedLogFileCountLimit", 14);
            TransactionScopeTimeout = TimeSpan.FromMinutes(GetAppSettingAsInt("TransactionScopeTimeoutMinutes", 120));
            PluginFolder = GetAppSetting("PluginFolder");

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
            if (string.IsNullOrEmpty(value)) return defaultValue;

            if (int.TryParse(value, out var iv)) return iv;
            return defaultValue;
        }

        public static string GetAppSetting(string key)
        {
            return ConfigurationManager.AppSettings[key + "-" + Environment.MachineName] ?? ConfigurationManager.AppSettings[key];
        }
    }
}