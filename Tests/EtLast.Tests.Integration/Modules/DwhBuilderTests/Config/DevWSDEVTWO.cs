namespace FizzCode.EtLast.Tests.Integration.Modules.DwhBuilderTests
{
    using System.Collections.Generic;

    internal class DevWSDEVTWO : IInstanceArgumentProvider
    {
        public string Instance => "WSDEVTWO";

        public Dictionary<string, object> Arguments => new()
        {
            ["DatabaseName"] = "EtLastIntegrationTest",
            ["ConnectionString"] = new LightWeight.AdoNet.NamedConnectionString("test", "System.Data.SqlClient", "Data Source=(local);Initial Catalog=\"EtLastIntegrationTest\";Integrated Security=SSPI;Connection Timeout=5", "2016"),
        };
    }
}