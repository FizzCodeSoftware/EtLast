namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public class StoredProcedureAdoNetDbReaderTests : AbstractEtlTask
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
            .CustomSqlStatement(() => new CustomSqlStatement(Context)
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
                .ReadFromStoredProcedure(new StoredProcedureAdoNetDbReader(Context)
                {
                    Name = "CallProcedure",
                    ConnectionString = ConnectionString,
                    Sql = "StoredProcedureAdoNetDbReaderTest",
                    MainTableName = null,
                })
                .Build().TakeRowsAndReleaseOwnership(this).ToList();

                Assert.AreEqual(2, result.Count);
                Assert.That.ExactMatch(result, new List<CaseInsensitiveStringKeyDictionary<object>>() {
                    new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 1, ["Value"] = "etlast" },
                    new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 2, ["Value"] = "StoredProcedureAdoNetDbReaderTest" }
                });
            });
    }
}
