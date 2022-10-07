namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public class StoredProcedureAdoNetDbReaderTests : AbstractEtlTask
{
    public NamedConnectionString ConnectionString { get; init; }

    public override void ValidateParameters()
    {
        if (ConnectionString == null)
            throw new ProcessParameterNullException(this, nameof(ConnectionString));
    }

    public override IEnumerable<IProcess> CreateJobs()
    {
        yield return new CustomSqlStatement(Context)
        {
            Name = "CreateProcedure",
            ConnectionString = ConnectionString,
            SqlStatement = "CREATE PROCEDURE StoredProcedureAdoNetDbReaderTest AS " +
                    "SELECT 1 AS Id, 'etlast' AS Value " +
                    "UNION " +
                    "SELECT 2 AS Id, 'StoredProcedureAdoNetDbReaderTest' AS Value",
        };

        yield return new CustomJob(Context)
        {
            Name = "CheckProcedureResult",
            Action = job =>
            {
                var result = SequenceBuilder.Fluent
                .ReadFromStoredProcedure(new StoredProcedureAdoNetDbReader(Context)
                {
                    Name = "CallProcedure",
                    ConnectionString = ConnectionString,
                    Sql = "StoredProcedureAdoNetDbReaderTest"
                })
                .Build().TakeRowsAndReleaseOwnership(this).ToList();

                Assert.AreEqual(2, result.Count);
                Assert.That.ExactMatch(result, new List<CaseInsensitiveStringKeyDictionary<object>>() {
                    new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 1, ["Value"] = "etlast" },
                    new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 2, ["Value"] = "StoredProcedureAdoNetDbReaderTest" }
                });
            }
        };
    }
}
