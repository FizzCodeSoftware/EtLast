namespace FizzCode.EtLast.Tests.Integration.Modules.DwhBuilderTests;

using System.Collections.Generic;

/// <summary>
/// Normally this file does not exists in the source code repository, only on the production environment(s).
/// </summary>
internal class Production : IDefaultArgumentProvider
{
    public Dictionary<string, object> Arguments => new()
    {
        ["ConnectionString"] = () => new LightWeight.AdoNet.NamedConnectionString("test", "Microsoft.Data.SqlClient", "Data Source=(local);Initial Catalog=\"EtLastIntegrationTest\";Integrated Security=SSPI;Connection Timeout=5;Encrypt=False", "2016"),
    };
}
