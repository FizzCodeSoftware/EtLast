namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public class Exception : AbstractEtlTask
{
    [ProcessParameterNullException]
    public NamedConnectionString ConnectionString { get; init; }

    public override void Execute(IFlow flow)
    {
        flow
            .CustomJob(nameof(Exception), job =>
            {
                throw new System.Exception("Test Exception.");
            });
    }
}
