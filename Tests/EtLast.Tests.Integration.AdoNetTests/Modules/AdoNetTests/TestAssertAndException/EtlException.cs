namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public class EtlException : AbstractEtlTask
{
    public NamedConnectionString ConnectionString { get; init; }
    public override void ValidateParameters()
    {
        if (ConnectionString == null)
            throw new ProcessParameterNullException(this, nameof(ConnectionString));
    }

    public override IEnumerable<IJob> CreateJobs()
    {
        yield return new CustomJob(Context)
        {
            Name = nameof(EtlException),
            Action = proc =>
            {
                var process = new EtLast.StoredProcedureAdoNetDbReader(Context)
                {
                    ConnectionString = ConnectionString,
                    Sql = "NotExisting_StoredProcedure"
                };

                var result = process.Evaluate(this).TakeRowsAndTransferOwnership().ToList();

                var exceptions = Context.GetExceptions();

                Assert.AreEqual(1, Context.ExceptionCount);
            }
        };
    }
}
