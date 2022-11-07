namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public class EtlException : AbstractEtlTask
{
    public NamedConnectionString ConnectionString { get; init; }
    public override void ValidateParameters()
    {
        if (ConnectionString == null)
            throw new ProcessParameterNullException(this, nameof(ConnectionString));
    }

    public override IEnumerable<IProcess> CreateJobs()
    {
        yield return new CustomJob(Context)
        {
            Name = nameof(EtlException),
            Action = job =>
            {
                var process = new StoredProcedureAdoNetDbReader(Context)
                {
                    ConnectionString = ConnectionString,
                    Sql = "NotExisting_StoredProcedure"
                };

                var result = process.TakeRowsAndTransferOwnership(this).ToList();

                Assert.AreEqual(1, process.Pipe.Exceptions.Count);
            }
        };
    }
}