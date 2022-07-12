namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public class StoredProcedureAdoNetDbReader : AbstractEtlTask
{
    public NamedConnectionString ConnectionString { get; init; }

    public override void ValidateParameters()
    {
        if (ConnectionString == null)
            throw new ProcessParameterNullException(this, nameof(ConnectionString));
    }

    public override IEnumerable<IExecutable> CreateProcesses()
    {
        yield return new CustomSqlStatement(Context)
        {
            ConnectionString = ConnectionString,
            SqlStatement = "CREATE PROCEDURE StoredProcedureAdoNetDbReaderTest AS " +
                    "SELECT 1 AS Id, 'etlast' AS Value " +
                    "UNION " +
                    "SELECT 2 AS Id, 'StoredProcedureAdoNetDbReaderTest' AS Value",
        };

        yield return new CustomAction(Context)
        {
            Name = "StoredProcedureAdoNetDbReader",
            Action = proc =>
            {
                var result = ProcessBuilder.Fluent.ReadFromStoredProcedure(
                new EtLast.StoredProcedureAdoNetDbReader(Context)
                {
                    ConnectionString = ConnectionString,
                    Sql = "StoredProcedureAdoNetDbReaderTest"
                })
                .Build().Evaluate(this).TakeRowsAndReleaseOwnership().ToList();
                

                Assert.AreEqual(2, result.Count);
                Assert.That.ExactMatch(result, new List<CaseInsensitiveStringKeyDictionary<object>>() {
                    new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 1, ["Value"] = "etlast" },
                    new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 2, ["Value"] = "StoredProcedureAdoNetDbReaderTest" }
                });
            }
        };
    }
}
