namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public class GetTableRecordCountTests : AbstractEtlTask
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
            SqlStatement = $"CREATE TABLE {nameof(GetTableRecordCountTests)} (Id INT NOT NULL, DateTimeValue DATETIME2);" +
                    $"INSERT INTO {nameof(GetTableRecordCountTests)} (Id, DateTimeValue) VALUES (1, '2022.07.08');" +
                    $"INSERT INTO {nameof(GetTableRecordCountTests)} (Id, DateTimeValue) VALUES (2, '2022.07.09');",
        };

        yield return new CustomJob(Context)
        {
            Name = nameof(GetTableRecordCountTests),
            Action = job =>
            {
                var result = new EtLast.GetTableRecordCount(Context)
                {
                    ConnectionString = ConnectionString,
                    TableName = ConnectionString.Escape(nameof(GetTableRecordCountTests)),
                }.ExecuteWithResult();

                Assert.AreEqual(2, result);
            }
        };
    }
}
