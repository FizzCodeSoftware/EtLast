namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public class CopyTableIntoExistingTableTests : AbstractEtlTask
{
    [ProcessParameterMustHaveValue]
    public NamedConnectionString ConnectionString { get; init; }

    public override void Execute(IFlow flow)
    {
        flow
            .CustomSqlStatement(() => new CustomSqlStatement(Context)
            {
                Name = "CreateSourceTable",
                ConnectionString = ConnectionString,
                SqlStatement = $"CREATE TABLE {nameof(CopyTableIntoExistingTableTests)} (Id INT NOT NULL, Value NVARCHAR(255));" +
                    $"INSERT INTO {nameof(CopyTableIntoExistingTableTests)} (Id, Value) VALUES (1, 'etlast');" +
                    $"INSERT INTO {nameof(CopyTableIntoExistingTableTests)} (Id, Value) VALUES (2, 'CopyTableIntoExistingTableTest');",
                MainTableName = nameof(CopyTableIntoExistingTableTests),
            })
            .CustomSqlStatement(() => new CustomSqlStatement(Context)
            {
                Name = "CreateTargetTable",
                ConnectionString = ConnectionString,
                SqlStatement = $"CREATE TABLE {nameof(CopyTableIntoExistingTableTests)}Target (Id INT NOT NULL, Value NVARCHAR(255));",
                MainTableName = nameof(CopyTableIntoExistingTableTests) + "Target",
            })
            .CopyTableIntoExistingTable(() => new CopyTableIntoExistingTable(Context)
            {
                ConnectionString = ConnectionString,
                Configuration = new TableCopyConfiguration()
                {
                    SourceTableName = nameof(CopyTableIntoExistingTableTests),
                    TargetTableName = $"{nameof(CopyTableIntoExistingTableTests)}Target",
                }
            })
            .ExecuteSequenceAndTakeRows(out var result, builder => builder
                .ReadFrom(new AdoNetDbReader(Context)
                {
                    Name = "ReadTargetTableContents",
                    ConnectionString = ConnectionString,
                    TableName = $"{nameof(CopyTableIntoExistingTableTests)}Target"
                })
                )
            .CustomJob("TestResult", job =>
            {
                Assert.AreEqual(2, result.Count);
                Assert.That.ExactMatch(result,
                [
                    new() { ["Id"] = 1, ["Value"] = "etlast" },
                    new() { ["Id"] = 2, ["Value"] = "CopyTableIntoExistingTableTest" }
                ]);
            });
    }
}
