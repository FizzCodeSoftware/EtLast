namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public class CopyTableIntoExistingTableTests : AbstractEtlTask
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
            Name = "Create source table",
            ConnectionString = ConnectionString,
            SqlStatement = $"CREATE TABLE {nameof(CopyTableIntoExistingTableTests)} (Id INT NOT NULL, Value NVARCHAR(255));" +
                    $"INSERT INTO {nameof(CopyTableIntoExistingTableTests)} (Id, Value) VALUES (1, 'etlast');" +
                    $"INSERT INTO {nameof(CopyTableIntoExistingTableTests)} (Id, Value) VALUES (2, 'CopyTableIntoExistingTableTest');",
        };

        yield return new CustomSqlStatement(Context)
        {
            Name = "Create target table",
            ConnectionString = ConnectionString,
            SqlStatement = $"CREATE TABLE {nameof(CopyTableIntoExistingTableTests)}Target (Id INT NOT NULL, Value NVARCHAR(255));"
        };

        yield return new CopyTableIntoExistingTable(Context)
        {
            ConnectionString = ConnectionString,
            Configuration = new TableCopyConfiguration()
            {
                SourceTableName = $"{nameof(CopyTableIntoExistingTableTests)}",
                TargetTableName = $"{nameof(CopyTableIntoExistingTableTests)}Target"
            }
        };

        yield return new CustomJob(Context)
        {
            Name = "Check target table contents",
            Action = job =>
            {
                var result = SequenceBuilder.Fluent
                .ReadFrom(new AdoNetDbReader(Context)
                {
                    Name = "Read target table contents",
                    ConnectionString = ConnectionString,
                    TableName = $"{nameof(CopyTableIntoExistingTableTests)}Target"
                }).Build().TakeRowsAndReleaseOwnership(this).ToList();

                Assert.AreEqual(2, result.Count);
                Assert.That.ExactMatch(result, new List<CaseInsensitiveStringKeyDictionary<object>>()
                {
                    new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 1, ["Value"] = "etlast" },
                    new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 2, ["Value"] = "CopyTableIntoExistingTableTest" }
                });
            }
        };
    }
}
