namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public class CopyTableIntoExistingTableTests : AbstractEtlTask
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
            .ContinueWith(() => new CustomSqlStatement(Context)
            {
                Name = "CreateSourceTable",
                ConnectionString = ConnectionString,
                SqlStatement = $"CREATE TABLE {nameof(CopyTableIntoExistingTableTests)} (Id INT NOT NULL, Value NVARCHAR(255));" +
                    $"INSERT INTO {nameof(CopyTableIntoExistingTableTests)} (Id, Value) VALUES (1, 'etlast');" +
                    $"INSERT INTO {nameof(CopyTableIntoExistingTableTests)} (Id, Value) VALUES (2, 'CopyTableIntoExistingTableTest');",
                MainTableName = nameof(CopyTableIntoExistingTableTests),
            })
            .ContinueWith(() => new CustomSqlStatement(Context)
            {
                Name = "CreateTargetTable",
                ConnectionString = ConnectionString,
                SqlStatement = $"CREATE TABLE {nameof(CopyTableIntoExistingTableTests)}Target (Id INT NOT NULL, Value NVARCHAR(255));",
                MainTableName = nameof(CopyTableIntoExistingTableTests) + "Target",
            })
            .ContinueWith(() => new CopyTableIntoExistingTable(Context)
            {
                ConnectionString = ConnectionString,
                Configuration = new TableCopyConfiguration()
                {
                    SourceTableName = nameof(CopyTableIntoExistingTableTests),
                    TargetTableName = $"{nameof(CopyTableIntoExistingTableTests)}Target",
                }
            })
            .ContinueWith(() => new CustomJob(Context)
            {
                Name = "CheckTargetTableContents",
                Action = job =>
                {
                    var result = SequenceBuilder.Fluent
                    .ReadFrom(new AdoNetDbReader(Context)
                    {
                        Name = "ReadTargetTableContents",
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
            });
    }
}
