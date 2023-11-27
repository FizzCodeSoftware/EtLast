namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public class EtlException : AbstractEtlTask
{
    [ProcessParameterMustHaveValue]
    public NamedConnectionString ConnectionString { get; init; }

    public override void Execute(IFlow flow)
    {
        flow
            .CustomJob(nameof(EtlException), job =>
            {
                var process = new StoredProcedureAdoNetDbReader()
                {
                    ConnectionString = ConnectionString,
                    Sql = "NotExisting_StoredProcedure",
                    MainTableName = null,
                };

                var result = process.TakeRowsAndTransferOwnership(this).ToList();

                Assert.AreEqual(1, process.FlowState.Exceptions.Count);
            });
    }
}