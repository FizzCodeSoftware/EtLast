namespace FizzCode.EtLast.Tests.Integration.Modules.DwhBuilderTests
{
    using System.Collections.Generic;

    /// <summary>
    /// Normally this file does not exists in the source code repository, only on the production environment(s).
    /// </summary>
    internal class Prod : IDefaultArgumentProvider
    {
        public Dictionary<string, object> Arguments => new()
        {
            ["DatabaseName"] = "EtLastIntegrationTest",
            ["ConnectionString"] = new LightWeight.AdoNet.NamedConnectionString("test", "System.Data.SqlClient", "Data Source=(local);Initial Catalog=\"EtLastIntegrationTest\";Integrated Security=SSPI;Connection Timeout=5", "2016"),
        };
    }
}