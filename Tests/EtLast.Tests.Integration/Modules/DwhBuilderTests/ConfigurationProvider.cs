namespace FizzCode.EtLast.Tests.Integration.Modules.DwhBuilderTests
{
    using System.Collections.Generic;

    public class ConfigurationProvider : IConfigurationProvider
    {
        public Dictionary<string, object> GetConfigurationValues(string instance)
        {
            return instance.ToLowerInvariant() switch
            {
                "wsdevtwo" => new()
                {
                    ["DatabaseName"] = "EtLastIntegrationTest",
                    ["ConnectionString"] = "Data Source=(local);Initial Catalog=\"EtLastIntegrationTest\";Integrated Security=SSPI;Connection Timeout=5",
                },
                _ => new()
                {
                    ["DatabaseName"] = "EtLastIntegrationTest",
                    ["ConnectionString"] = "Data Source=(local);Initial Catalog=\"EtLastIntegrationTest\";Integrated Security=SSPI;Connection Timeout=5",
                }
            };
        }
    }
}