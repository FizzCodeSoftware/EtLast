namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public class Exception : AbstractEtlTask
{
    public NamedConnectionString ConnectionString { get; init; }
    public override void ValidateParameters()
    {
        if (ConnectionString == null)
            throw new ProcessParameterNullException(this, nameof(ConnectionString));
    }

    public override IEnumerable<IProcess> CreateJobs(IProcess caller)
    {
        yield return new CustomJob(Context)
        {
            Name = nameof(Exception),
            Action = job =>
            {
                throw new System.Exception("Test Exception.");
            }
        };
    }
}
