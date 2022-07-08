namespace FizzCode.EtLast.Tests.Integration.Modules.DwhBuilderTests.Arguments;

/// <summary>
/// If the <see cref="IInstanceArgumentProvider.Instance"/> name matches to the execution environment's name then
/// the values found in them will override the values found in <see cref="IDefaultArgumentProvider"/> implementations.
/// </summary>
internal class ExampleInstanceArgumentProviderForMachine1 : IInstanceArgumentProvider
{
    public string Instance => "NONEXISTINGEXAMPLEMACHINE1";

    public Dictionary<string, object> Arguments => new()
    {
        ["ConnectionString"] = () => new NamedConnectionString("test", "Microsoft.Data.SqlClient", "Data Source=(local);Initial Catalog=\"EtLastIntegrationTest\";Integrated Security=SSPI;Connection Timeout=5;Encrypt=False", "2016"),
        ["ConnectionStringMaster"] = () => new NamedConnectionString("test", "Microsoft.Data.SqlClient", "Data Source=(local);Initial Catalog=\"master\";Integrated Security=SSPI;Connection Timeout=5;Encrypt=False", "2016"),
    };
}