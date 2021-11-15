namespace FizzCode.EtLast.ConsoleHost
{
    using System;
    using System.Collections.Generic;
    using FizzCode.LightWeight.Configuration;
    using Microsoft.Extensions.Configuration;

    public class HostConfiguration
    {
        public string CustomReferenceAssemblyFolder { get; set; }
        public string ModulesFolder { get; set; } = @".\modules";
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

            ModulesFolder = ConfigurationReader.GetCurrentValue(configuration, section, "ModulesFolder", @".\modules", SecretProtector);

            CustomReferenceAssemblyFolder = ConfigurationReader.GetCurrentValue(configuration, section, "CustomReferenceAssemblyFolder", null, SecretProtector);

            GetCommandAliases(configuration, section);
        }

        public List<IEtlSessionListener> GetSessionListeners(IEtlSession session)
        {
            var result = new List<IEtlSessionListener>();

            var listenersSection = _configuration.GetSection(_section + ":SessionListeners");
            if (listenersSection == null)
                return result;

            var children = listenersSection.GetChildren();
            foreach (var childSection in children)
            {
                if (!ConfigurationReader.GetCurrentValue(childSection, "Enabled", false))
                    continue;

                var type = Type.GetType(childSection.Key);
                if (type != null && typeof(IEtlSessionListener).IsAssignableFrom(type))
                {
                    try
                    {
                        var instance = (IEtlSessionListener)Activator.CreateInstance(type);
                        var ok = instance.Init(session, childSection, SecretProtector);
                        if (ok)
                        {
                            result.Add(instance);
                        }
                    }
                    catch (Exception ex)
                    {
                        var exception = new Exception("Can't initialize session listener.", ex);
                        exception.Data.Add("FullyQualifiedTypeName", childSection.Key);
                        throw exception;
                    }
                }
                else
                {
                    var exception = new Exception("Session listener type not found.");
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