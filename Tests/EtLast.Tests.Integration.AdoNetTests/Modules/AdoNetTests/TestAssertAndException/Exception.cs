namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public class Exception : AbstractEtlTask
{
    public NamedConnectionString ConnectionString { get; init; }
    public override void ValidateParameters()
    {
        if (ConnectionString == null)
            throw new ProcessParameterNullException(this, nameof(ConnectionString));
    }
        
    public override IEnumerable<IExecutable> CreateProcesses()
    {
        yield return new CustomAction(Context)
        {
            Name = $"{nameof(Exception)}",
            Action = proc =>
            {
                throw new System.Exception("Test Exception.");
            }
        };
    }
}
