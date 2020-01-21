namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Reflection;
    using FizzCode.DbTools.Configuration;
    using Microsoft.Extensions.Configuration;

    public abstract class AbstractEtlPlugin : IEtlPlugin
    {
        private string _nameCached;
        public string Name => _nameCached ?? (_nameCached = GetType().GetFriendlyTypeName());

        public ModuleConfiguration ModuleConfiguration { get; private set; }
        public IEtlContext Context { get; private set; }

        protected TimeSpan TransactionScopeTimeout { get; private set; }

        public void Init(IEtlContext context, ModuleConfiguration moduleConfiguration)
        {
            ModuleConfiguration = moduleConfiguration;
            Context = context;
        }

        public virtual void BeforeExecute()
        {
        }

        public abstract void Execute();

        protected string GetStorageFolder(params string[] subFolders)
        {
            return GetPathFromConfiguration("StorageFolder", subFolders);
        }

        protected T GetModuleSetting<T>(string key, T defaultValue = default, string subSection = null)
        {
            var v = ModuleConfiguration.Configuration.GetValue<T>((subSection == null ? "Module" : "Module:" + subSection) + ":" + key + "-" + Environment.MachineName, default);
            if (v != null && !v.Equals(default(T)))
                return v;

            v = ModuleConfiguration.Configuration.GetValue((subSection == null ? "Module" : "Module:" + subSection) + ":" + key, defaultValue);
            if (v != null && !v.Equals(default(T)))
                return v;

            v = ModuleConfiguration.Configuration.GetValue<T>((subSection == null ? "Shared" : "Shared:" + subSection) + ":" + key + "-" + Environment.MachineName, default);
            if (v != null && !v.Equals(default(T)))
                return v;

            v = ModuleConfiguration.Configuration.GetValue<T>((subSection == null ? "Shared" : "Shared:" + subSection) + ":" + key, default);
            if (v != null && !v.Equals(default(T)))
                return v;

            return defaultValue;
        }

        protected string GetPathFromConfiguration(string appSettingName, params string[] subFolders)
        {
            var path = GetModuleSetting<string>(appSettingName);
            if (string.IsNullOrEmpty(path))
                return null;

            if (path.StartsWith(@".\", StringComparison.InvariantCultureIgnoreCase))
            {
                var exeFolder = Path.GetDirectoryName(Assembly.GetEntryAssembly().Location);
                path = Path.Combine(exeFolder, path.Substring(2));
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
            var fileName = GetModuleSetting<string>(appSettingName);
            if (string.IsNullOrEmpty(fileName))
                return null;

            if (fileName.StartsWith(@".\", StringComparison.InvariantCultureIgnoreCase))
            {
                var exeFolder = Path.GetDirectoryName(Assembly.GetEntryAssembly().Location);
                fileName = Path.Combine(exeFolder, fileName.Substring(2));
            }

            return fileName;
        }

        protected virtual ConnectionStringWithProvider GetConnectionString(string key, bool allowMachineNameOverride = true)
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