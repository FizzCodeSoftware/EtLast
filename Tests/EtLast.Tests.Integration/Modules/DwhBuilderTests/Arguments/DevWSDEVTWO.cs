namespace FizzCode.EtLast.Tests.Integration.Modules.DwhBuilderTests;

using System.Collections.Generic;

internal class DevWSDEVTWO : IInstanceArgumentProvider
{
    public string Instance => "WSDEVTWO";

    public Dictionary<string, object> Arguments => new()
    {
        ["ConnectionString"] = () => new LightWeight.AdoNet.NamedConnectionString("test", "Microsoft.Data.SqlClient", "Data Source=(local);Initial Catalog=\"EtLastIntegrationTest\";Integrated Security=SSPI;Connection Timeout=5;Encrypt=False", "2016"),
    };
}
