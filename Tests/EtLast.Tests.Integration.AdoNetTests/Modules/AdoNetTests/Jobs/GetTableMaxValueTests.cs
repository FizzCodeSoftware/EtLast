namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public class GetTableMaxValueTests : AbstractEtlTask
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
                Name = "CreateTableAndInsertContent",
                ConnectionString = ConnectionString,
                SqlStatement = $"CREATE TABLE {nameof(GetTableMaxValueTests)} (Id INT NOT NULL, DateTimeValue DATETIME2);" +
                    $"INSERT INTO {nameof(GetTableMaxValueTests)} (Id, DateTimeValue) VALUES (1, '2022.07.08');" +
                    $"INSERT INTO {nameof(GetTableMaxValueTests)} (Id, DateTimeValue) VALUES (1, '2022.07.09');",
                MainTableName = nameof(GetTableMaxValueTests),
            })
            .ContinueWith(() => new CustomJob(Context)
            {
                Name = "CheckMaxValue",
                Action = job =>
                {
                    var result = new GetTableMaxValue<DateTime>(Context)
                    {
                        Name = "GetTableMaxValue",
                        ConnectionString = ConnectionString,
                        TableName = ConnectionString.Escape(nameof(GetTableMaxValueTests)),
                        ColumnName = "DateTimeValue",
                        WhereClause = null,
                    }.ExecuteWithResult(job);

                    Assert.AreEqual(new DateTime(2022, 7, 9), result.MaxValue);
                }
            });
    }
}