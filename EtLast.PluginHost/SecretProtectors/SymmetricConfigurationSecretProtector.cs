namespace FizzCode.EtLast.PluginHost
{
    using FizzCode.DbTools.Configuration;
    using Microsoft.Extensions.Configuration;

    public class SymmetricConfigurationSecretProtector : IConfigurationSecretProtector
    {
        public string BaseKey { get; }
        public bool UseMachineName { get; }
        public bool UseUserName { get; }

        public SymmetricConfigurationSecretProtector(IConfigurationSection section)
        {
            BaseKey = section.GetValue<string>("BaseKey");
            UseMachineName = section.GetValue<bool>("UseMachineName");
            UseUserName = section.GetValue<bool>("UseUserName");
        }

        public string Decrypt(string value)
        {
            return value;
        }

        public string Encrypt(string value)
        {
            return value;
        }
    }
}