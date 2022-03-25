namespace FizzCode.EtLast.Tests.Integration.Modules.DwhBuilderTests;

using System;
using System.Collections.Generic;

internal class Default : IDefaultArgumentProvider
{
    public Dictionary<string, object> Arguments => new()
    {
        ["DatabaseName"] = "EtLastIntegrationTest",
        ["CreateDatabase:Definition"] = () => new TestDwhDefinition(),
        ["ExceptionTest:ExceptionType"] = typeof(Exception),
        ["ExceptionTest:Message"] = (IEtlSessionArguments args) =>
            "oops something went wrong while trowing fake exceptions while processing the database called ["
            + args.Get<string>("DatabaseName")
            + "]",
    };
}
