namespace FizzCode.EtLast.Tests.Integration.Modules.DwhBuilderTests
{
    using System;
    using System.Collections.Generic;

    internal class Default : IDefaultArgumentProvider
    {
        public Dictionary<string, object> Arguments => new()
        {
            ["DatabaseName"] = "EtLastIntegrationTest",
            ["CreateDatabase:Definition"] = () => new TestDwhDefinition(),
            ["ExceptionTest:ExceptionType"] = typeof(Exception),
        };
    }
}