namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public class Exception : AbstractEtlTask
{
    public NamedConnectionString ConnectionString { get; init; }
    public override void ValidateParameters()
    {
        if (ConnectionString == null)
            throw new ProcessParameterNullException(this, nameof(ConnectionString));
    }

    public override void Execute(IFlow flow)
    {
        flow
            .ContinueWith(() => new CustomJob(Context)
            {
                Name = nameof(Exception),
                Action = job =>
                {
                    throw new System.Exception("Test Exception.");
                }
            });
    }
}
