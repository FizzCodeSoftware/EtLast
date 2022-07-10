namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

internal class StoredProcedureAdoNetDbReader : AbstractEtlTask
{
    public NamedConnectionString ConnectionString { get; init; }
    public string DatabaseName { get; init; }

    public override void ValidateParameters()
    {
        if (ConnectionString == null)
            throw new ProcessParameterNullException(this, nameof(ConnectionString));

        if (DatabaseName == null)
            throw new ProcessParameterNullException(this, nameof(DatabaseName));
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
                var process = new EtLast.StoredProcedureAdoNetDbReader(Context)
                {
                    ConnectionString = ConnectionString,
                    Sql = "StoredProcedureAdoNetDbReaderTest"
                };

                var result = process.Evaluate(this).TakeRowsAndTransferOwnership().ToList();

                Assert.AreEqual(2, result.Count);
                Assert.AreEqual(result[0]["Id"], 1);
                Assert.AreEqual(result[0]["Value"], "etlast");
                Assert.AreEqual(result[1]["Id"], 2);
                Assert.AreEqual(result[1]["Value"], "StoredProcedureAdoNetDbReaderTest");
            }
        };
    }
}
