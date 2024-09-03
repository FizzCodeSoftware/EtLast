namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public class StoredProcedureAdoNetDbReaderTests : AbstractEtlTask
{
    [ProcessParameterMustHaveValue]
    public IAdoNetSqlConnectionString ConnectionString { get; init; }

    public override void Execute(IFlow flow)
    {
        flow
            .CustomSqlStatement(() => new CustomSqlStatement()
            {
                Name = "CreateProcedure",
                ConnectionString = ConnectionString,
                SqlStatement = "CREATE PROCEDURE StoredProcedureAdoNetDbReaderTest AS " +
                "SELECT 1 AS Id, 'etlast' AS Value " +
                "UNION " +
                "SELECT 2 AS Id, 'StoredProcedureAdoNetDbReaderTest' AS Value",
                MainTableName = "StoredProcedureAdoNetDbReaderTest",
            })
            .CustomJob("CheckProcedureResult", job =>
            {
                var result = SequenceBuilder.Fluent
                .ReadFromStoredProcedure(new StoredProcedureAdoNetDbReader()
                {
                    Name = "CallProcedure",
                    ConnectionString = ConnectionString,
                    Sql = "StoredProcedureAdoNetDbReaderTest",
                    MainTableName = null,
                })
                .Build().TakeRowsAndReleaseOwnership(this).ToList();

                Assert.AreEqual(2, result.Count);
                Assert.That.ExactMatch(result, [
                    new() { ["Id"] = 1, ["Value"] = "etlast" },
                    new() { ["Id"] = 2, ["Value"] = "StoredProcedureAdoNetDbReaderTest" }
                ]);
            });
    }
}
