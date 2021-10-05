namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Reflection;
    using FizzCode.LightWeight.AdoNet;
    using FizzCode.LightWeight.Configuration;

    public abstract class AbstractEtlPlugin : IEtlPlugin
    {
        private string _nameCached;
        public string Name => _nameCached ??= GetType().GetFriendlyTypeName();

        public ModuleConfiguration ModuleConfiguration { get; private set; }
        public IEtlContext Context => PluginTopic.Context;
        public ITopic PluginTopic { get; private set; }

        public void Init(ITopic topic, ModuleConfiguration moduleConfiguration)
        {
            ModuleConfiguration = moduleConfiguration;
            PluginTopic = topic;
        }

        public virtual void BeforeExecute()
        {
        }

        public abstract void Execute();

        protected string GetStorageFolder(params string[] subFolders)
        {
            return GetPathFromConfiguration("StorageFolder", subFolders);
        }

        protected T GetModuleSetting<T>(string key, T defaultValue, string subSection = null)
        {
            var value = ConfigurationReader.GetCurrentValue<T>(ModuleConfiguration.Configuration, subSection == null ? "Module" : "Module:" + subSection, key, default);

            if (value != null && !value.Equals(default(T)))
                return value;

            return ConfigurationReader.GetCurrentValue(ModuleConfiguration.Configuration, subSection == null ? "Shared" : "Shared:" + subSection, key, defaultValue);
        }

        protected string GetModuleSetting(string key, string defaultValue, string subSection = null)
        {
            var value = ConfigurationReader.GetCurrentValue(ModuleConfiguration.Configuration, subSection == null ? "Module" : "Module:" + subSection, key, null, ModuleConfiguration.SecretProtector);
            return value ?? ConfigurationReader.GetCurrentValue(ModuleConfiguration.Configuration, subSection == null ? "Shared" : "Shared:" + subSection, key, defaultValue);
        }

        protected string GetPathFromConfiguration(string appSettingName, params string[] subFolders)
        {
            var path = GetModuleSetting<string>(appSettingName, null);
            if (string.IsNullOrEmpty(path))
                return null;

            if (path.StartsWith(@".\", StringComparison.InvariantCultureIgnoreCase))
            {
                var exeFolder = Path.GetDirectoryName(Assembly.GetEntryAssembly().Location);
                path = Path.Combine(exeFolder, path[2..]);
            }

            if (subFolders?.Length > 0)
            {
                var l = new List<string>() { path };
                l.AddRange(subFolders);
                path = Path.Combine(l.ToArray());
            }

            if (!Directory.Exists(path))
            {
                Directory.CreateDirectory(path);
            }

            return path;
        }

        protected string GetFileNameFromConfiguration(string appSettingName)
        {
            var fileName = GetModuleSetting<string>(appSettingName, null);
            if (string.IsNullOrEmpty(fileName))
                return null;

            if (fileName.StartsWith(@".\", StringComparison.InvariantCultureIgnoreCase))
            {
                var exeFolder = Path.GetDirectoryName(Assembly.GetEntryAssembly().Location);
                fileName = Path.Combine(exeFolder, fileName[2..]);
            }

            return fileName;
        }

        protected virtual NamedConnectionString GetConnectionString(string key, bool allowMachineNameOverride = true)
        {
            if (ModuleConfiguration.ConnectionStrings == null)
                return null;

            if (allowMachineNameOverride)
            {
                var connectionString = ModuleConfiguration.ConnectionStrings[key + "-" + Environment.MachineName];
                if (connectionString != null)
                    return connectionString;
            }

            return ModuleConfiguration.ConnectionStrings[key];
        }
    }
}