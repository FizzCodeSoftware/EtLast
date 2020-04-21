namespace FizzCode.EtLast.PluginHost
{
    using System;
    using System.Collections.Generic;
    using Microsoft.Extensions.Configuration;
    using Serilog.Events;

    public enum DynamicCompilationMode { Never, Always, Default }

    public class HostConfiguration
    {
        public TimeSpan TransactionScopeTimeout { get; set; } = TimeSpan.FromMinutes(120);

        public Uri DiagnosticsUri { get; set; }
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
        public Dictionary<string, string> CommandAliases { get; set; } = new Dictionary<string, string>(StringComparer.InvariantCultureIgnoreCase);

        public void LoadFromConfiguration(IConfigurationRoot configuration, string section)
        {
            var diagUrl = GetHostSetting<string>(configuration, section, "RemoteDiagnostics:Url", null);
            if (!string.IsNullOrEmpty(diagUrl))
                DiagnosticsUri = new Uri(diagUrl);

            SeqUrl = GetHostSetting<string>(configuration, section, "Seq:Url", null);
            SeqApiKey = GetHostSetting<string>(configuration, section, "Seq:ApiKey", null);
            RetainedLogFileCountLimitImportant = GetHostSetting(configuration, section, "RetainedLogFileCountLimit:Important", 30);
            RetainedLogFileCountLimitInfo = GetHostSetting(configuration, section, "RetainedLogFileCountLimit:Info", 14);
            RetainedLogFileCountLimitLow = GetHostSetting(configuration, section, "RetainedLogFileCountLimit:Low", 4);
            TransactionScopeTimeout = TimeSpan.FromMinutes(GetHostSetting(configuration, section, "TransactionScopeTimeoutMinutes", 120));
            ModulesFolder = GetHostSetting(configuration, section, "ModulesFolder", @".\modules");

            var v = GetHostSetting(configuration, section, "DynamicCompilation:Mode", "Default");
            if (!string.IsNullOrEmpty(v) && Enum.TryParse(v, out DynamicCompilationMode mode))
            {
                DynamicCompilationMode = mode;
            }

            v = GetHostSetting(configuration, section, "MinimumLogLevel:Console", "Information");
            if (!string.IsNullOrEmpty(v) && Enum.TryParse(v, out LogEventLevel level))
            {
                MinimumLogLevelOnConsole = level;
            }

            v = GetHostSetting(configuration, section, "MinimumLogLevel:File", "Debug");
            if (!string.IsNullOrEmpty(v) && Enum.TryParse(v, out level))
            {
                MinimumLogLevelInFile = level;
            }

            v = GetHostSetting(configuration, section, "MinimumLogLevel:IoFile", "Verbose");
            if (!string.IsNullOrEmpty(v) && Enum.TryParse(v, out level))
            {
                MinimumLogLevelIo = level;
            }

            GetCommandAliases(configuration, section);
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

        private static T GetHostSetting<T>(IConfigurationRoot configuration, string section, string key, T defaultValue)
        {
            var v = configuration.GetValue<T>(section + ":" + key + "-" + Environment.MachineName, default);
            if (v != null && !v.Equals(default(T)))
                return v;

            if (configuration.GetValue<object>(section + ":" + key) == null)
            {
                var c = Console.ForegroundColor;
                Console.ForegroundColor = ConsoleColor.Gray;
                Console.WriteLine("missing host configuration entry: " + section + ":" + key + ", using default value: " + defaultValue);
                Console.ForegroundColor = c;
            }

            return configuration.GetValue(section + ":" + key, defaultValue);
        }
    }
}