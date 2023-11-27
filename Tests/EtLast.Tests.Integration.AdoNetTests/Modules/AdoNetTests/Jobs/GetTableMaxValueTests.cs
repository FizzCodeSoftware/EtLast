namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public class GetTableMaxValueTests : AbstractEtlTask
{
    [ProcessParameterMustHaveValue]
    public NamedConnectionString ConnectionString { get; init; }

    public override void Execute(IFlow flow)
    {
        flow
            .CustomSqlStatement(() => new CustomSqlStatement()
            {
                Name = "CreateTableAndInsertContent",
                ConnectionString = ConnectionString,
                SqlStatement = $"CREATE TABLE {nameof(GetTableMaxValueTests)} (Id INT NOT NULL, DateTimeValue DATETIME2);" +
                    $"INSERT INTO {nameof(GetTableMaxValueTests)} (Id, DateTimeValue) VALUES (1, '2022.07.08');" +
                    $"INSERT INTO {nameof(GetTableMaxValueTests)} (Id, DateTimeValue) VALUES (1, '2022.07.09');",
                MainTableName = nameof(GetTableMaxValueTests),
            })
            .GetTableMaxValue(out var result, () => new GetTableMaxValue<DateTime>()
            {
                Name = "GetTableMaxValue",
                ConnectionString = ConnectionString,
                TableName = ConnectionString.Escape(nameof(GetTableMaxValueTests)),
                ColumnName = "DateTimeValue",
                WhereClause = null,
            })
            .CustomJob("Test", job =>
            {
                Assert.AreEqual(new DateTime(2022, 7, 9), result.MaxValue);
            });
    }
}