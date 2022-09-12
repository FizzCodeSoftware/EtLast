namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public class GetTableRecordCountTests : AbstractEtlTask
{
    public NamedConnectionString ConnectionString { get; init; }

    public override void ValidateParameters()
    {
        if (ConnectionString == null)
            throw new ProcessParameterNullException(this, nameof(ConnectionString));
    }

    public override IEnumerable<IJob> CreateJobs()
    {
        yield return new CustomSqlStatement(Context)
        {
            Name = "Create table and insert content",
            ConnectionString = ConnectionString,
            SqlStatement = $"CREATE TABLE {nameof(GetTableRecordCountTests)} (Id INT NOT NULL, DateTimeValue DATETIME2);" +
                    $"INSERT INTO {nameof(GetTableRecordCountTests)} (Id, DateTimeValue) VALUES (1, '2022.07.08');" +
                    $"INSERT INTO {nameof(GetTableRecordCountTests)} (Id, DateTimeValue) VALUES (2, '2022.07.09');",
        };

        yield return new CustomJob(Context)
        {
            Name = "Check record count",
            Action = job =>
            {
                var result = new GetTableRecordCount(Context)
                {
                    Name = "Get record count",
                    ConnectionString = ConnectionString,
                    TableName = ConnectionString.Escape(nameof(GetTableRecordCountTests)),
                }.ExecuteWithResult(job);

                Assert.AreEqual(2, result);
            }
        };
    }
}
