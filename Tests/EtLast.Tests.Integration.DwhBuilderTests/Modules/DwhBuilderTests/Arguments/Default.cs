namespace FizzCode.EtLast.Tests.Integration.Modules.DwhBuilderTests;

internal class Default : IDefaultArgumentProvider
{
    public Dictionary<string, object> Arguments => new()
    {
        ["DatabaseName"] = "EtLastIntegrationTest",
        ["CreateDatabase:Definition"] = () => new TestDwhDefinition(),
        ["ExceptionTest:ExceptionType"] = typeof(Exception),
        ["ExceptionTest:Message"] = (IArgumentCollection args) =>
            "oops something went wrong while trowing fake exceptions while processing the database called ["
            + args.Get<string>("DatabaseName")
            + "]",
        ["ConnectionString"] = () => new NamedConnectionString("test", "Microsoft.Data.SqlClient", "Data Source=(local);Initial Catalog=\"EtLastIntegrationTest\";Integrated Security=SSPI;Connection Timeout=5;Encrypt=False", "2016"),
        ["ConnectionStringMaster"] = () => new NamedConnectionString("test", "Microsoft.Data.SqlClient", "Data Source=(local);Initial Catalog=\"master\";Integrated Security=SSPI;Connection Timeout=5;Encrypt=False", "2016"),
    };
}