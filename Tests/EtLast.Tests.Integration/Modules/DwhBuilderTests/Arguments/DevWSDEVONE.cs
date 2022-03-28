namespace FizzCode.EtLast.Tests.Integration.Modules.DwhBuilderTests;

internal class DevWSDEVONE : IInstanceArgumentProvider
{
    public string Instance => "WSDEVONE";

    public Dictionary<string, object> Arguments => new()
    {
        ["ConnectionString"] = () => new NamedConnectionString("test", "Microsoft.Data.SqlClient", "Data Source=(local);Initial Catalog=\"EtLastIntegrationTest\";Integrated Security=SSPI;Connection Timeout=5;Encrypt=False", "2016"),
    };
}
