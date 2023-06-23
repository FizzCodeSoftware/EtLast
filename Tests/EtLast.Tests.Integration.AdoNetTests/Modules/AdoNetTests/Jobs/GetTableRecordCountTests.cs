namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public class GetTableRecordCountTests : AbstractEtlTask
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
            .ExecuteProcess(() => new CustomSqlStatement(Context)
            {
                Name = "CreateTableAndInsertContent",
                ConnectionString = ConnectionString,
                SqlStatement = $"CREATE TABLE {nameof(GetTableRecordCountTests)} (Id INT NOT NULL, DateTimeValue DATETIME2);" +
                    $"INSERT INTO {nameof(GetTableRecordCountTests)} (Id, DateTimeValue) VALUES (1, '2022.07.08');" +
                    $"INSERT INTO {nameof(GetTableRecordCountTests)} (Id, DateTimeValue) VALUES (2, '2022.07.09');",
                MainTableName = nameof(GetTableRecordCountTests),
            })
            .ExecuteProcess(() => new CustomJob(Context)
            {
                Name = "CheckRecordCount",
                Action = job =>
                {
                    var result = new GetTableRecordCount(Context)
                    {
                        Name = "GetRecordCount",
                        ConnectionString = ConnectionString,
                        TableName = ConnectionString.Escape(nameof(GetTableRecordCountTests)),
                        WhereClause = null,
                    }.ExecuteWithResult(job);

                    Assert.AreEqual(2, result);
                }
            });
    }
}