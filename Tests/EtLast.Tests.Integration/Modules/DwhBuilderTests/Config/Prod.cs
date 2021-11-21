namespace FizzCode.EtLast.Tests.Integration.Modules.DwhBuilderTests
{
    using System.Collections.Generic;

    /// <summary>
    /// Normally this file does not exists in the source code repository, only on the production environments.
    /// </summary>
    internal class Prod : IDefaultConfigurationProvider
    {
        public Dictionary<string, object> GetConfiguration()
        {
            return new()
            {
                ["DatabaseName"] = "EtLastIntegrationTest",
                ["ConnectionString"] = "Data Source=(local);Initial Catalog=\"EtLastIntegrationTest\";Integrated Security=SSPI;Connection Timeout=5",
            };
        }
    }
}