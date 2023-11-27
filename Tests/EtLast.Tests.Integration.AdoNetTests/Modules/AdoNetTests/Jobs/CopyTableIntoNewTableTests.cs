namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public class CopyTableIntoNewTableTests : AbstractEtlTask
{
    [ProcessParameterMustHaveValue]
    public NamedConnectionString ConnectionString { get; init; }

    public override void Execute(IFlow flow)
    {
        flow
            .CustomSqlStatement(() => new CustomSqlStatement()
            {
                Name = "CreateSourceTable",
                ConnectionString = ConnectionString,
                SqlStatement = $"CREATE TABLE {nameof(CopyTableIntoNewTableTests)} (Id INT NOT NULL, Value NVARCHAR(255));" +
                    $"INSERT INTO {nameof(CopyTableIntoNewTableTests)} (Id, Value) VALUES (1, 'etlast');" +
                    $"INSERT INTO {nameof(CopyTableIntoNewTableTests)} (Id, Value) VALUES (2, 'CopyTableIntoExistingTableTest');",
                MainTableName = nameof(CopyTableIntoNewTableTests),
            })
            .CopyTableIntoNewTable(() => new CopyTableIntoNewTable()
            {
                ConnectionString = ConnectionString,
                Configuration = new TableCopyConfiguration()
                {
                    SourceTableName = nameof(CopyTableIntoNewTableTests),
                    TargetTableName = $"{nameof(CopyTableIntoNewTableTests)}Target"
                }
            })
            .ExecuteSequenceAndTakeRows(out var result, builder => builder
                .ReadFrom(new AdoNetDbReader()
                {
                    Name = "Read target table contents",
                    ConnectionString = ConnectionString,
                    TableName = $"{nameof(CopyTableIntoNewTableTests)}Target"
                })
            )
            .CustomJob("Test", job =>
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