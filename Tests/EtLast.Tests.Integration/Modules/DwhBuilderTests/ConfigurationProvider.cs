namespace FizzCode.EtLast.Tests.Integration.Modules.DwhBuilderTests
{
    using System.Collections.Generic;

    public class ConfigurationProvider : IConfigurationProvider
    {
        public Dictionary<string, object> GetConfigurationValues(string instance)
        {
            return new()
            {
                ["ConnectionString"] = instance.ToLowerInvariant() switch
                {
                    "wsdevtwo" => "Data Source=(local);Initial Catalog=\"EtLastIntegrationTest\";Integrated Security=SSPI;Connection Timeout=5",
                    _ => "Data Source=(local);Initial Catalog=\"EtLastIntegrationTest\";Integrated Security=SSPI;Connection Timeout=5",
                }
            };
        }
    }
}