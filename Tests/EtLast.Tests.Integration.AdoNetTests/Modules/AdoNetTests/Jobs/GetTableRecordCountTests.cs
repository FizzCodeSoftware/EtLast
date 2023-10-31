namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public class GetTableRecordCountTests : AbstractEtlTask
{
    [ProcessParameterNullException]
    public NamedConnectionString ConnectionString { get; init; }

    public override void Execute(IFlow flow)
    {
        flow
            .CustomSqlStatement(() => new CustomSqlStatement(Context)
            {
                Name = "CreateTableAndInsertContent",
                ConnectionString = ConnectionString,
                SqlStatement = $"CREATE TABLE {nameof(GetTableRecordCountTests)} (Id INT NOT NULL, DateTimeValue DATETIME2);" +
                    $"INSERT INTO {nameof(GetTableRecordCountTests)} (Id, DateTimeValue) VALUES (1, '2022.07.08');" +
                    $"INSERT INTO {nameof(GetTableRecordCountTests)} (Id, DateTimeValue) VALUES (2, '2022.07.09');",
                MainTableName = nameof(GetTableRecordCountTests),
            })
            .GetTableRecordCount(out var result, () => new GetTableRecordCount(Context)
            {
                Name = "GetRecordCount",
                ConnectionString = ConnectionString,
                TableName = ConnectionString.Escape(nameof(GetTableRecordCountTests)),
                WhereClause = null,
            })
            .CustomJob("Test", job =>
            {
                Assert.AreEqual(2, result);
            });
    }
}