namespace FizzCode.EtLast.Tests.Integration.Modules.DwhBuilderTests
{
    using System.Collections.Generic;

    internal class DevWSDEVTWO : IInstanceConfigurationProvider
    {
        public string Instance => "WSDEVTWO";

        public Dictionary<string, object> Configuration => new()
        {
            ["DatabaseName"] = "EtLastIntegrationTest",
            ["ConnectionString"] = "Data Source=(local);Initial Catalog=\"EtLastIntegrationTest\";Integrated Security=SSPI;Connection Timeout=5",
        };
    }
}