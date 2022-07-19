namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public class GetTableMaxValue : AbstractEtlTask
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

    public override IEnumerable<IJob> CreateJobs()
    {
        yield return new CustomSqlStatement(Context)
        {
            ConnectionString = ConnectionString,
            SqlStatement = $"CREATE TABLE {nameof(GetTableMaxValue)} (Id INT NOT NULL, DateTimeValue DATETIME2);" +
                    $"INSERT INTO {nameof(GetTableMaxValue)} (Id, DateTimeValue) VALUES (1, '2022.07.08');" +
                    $"INSERT INTO {nameof(GetTableMaxValue)} (Id, DateTimeValue) VALUES (1, '2022.07.09');",
        };

        yield return new CustomJob(Context)
        {
            Name = nameof(GetTableMaxValue),
            Action = job =>
            {
                var result = new GetTableMaxValue<DateTime>(Context)
                {
                    ConnectionString = ConnectionString,
                    TableName = ConnectionString.Escape(nameof(GetTableMaxValue)),
                    ColumnName = "DateTimeValue",
                }.ExecuteWithResult();

                Assert.AreEqual(new DateTime(2022, 7, 9), result.MaxValue);
            }
        };
    }
}
