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

    public override IEnumerable<IExecutable> CreateProcesses()
    {
        yield return new CustomSqlStatement(Context)
        {
            ConnectionString = ConnectionString,
            SqlStatement = "CREATE TABLE GetTableMaxValue (Id INT NOT NULL, DateTimeValue DATETIME2);" +
                    "INSERT INTO GetTableMaxValue (Id, DateTimeValue) VALUES (1, '2022.07.08');" +
                    "INSERT INTO GetTableMaxValue (Id, DateTimeValue) VALUES (1, '2022.07.09');",
        };
        
        yield return new CustomAction(Context)
        {
            Name = "GetTableMaxValue",
            Action = proc =>
            {
                var result = new GetTableMaxValue<DateTime>(Context)
                {
                    ConnectionString = ConnectionString,
                    TableName = ConnectionString.Escape("GetTableMaxValue"),
                    ColumnName = "DateTimeValue",
                }.Execute();

                Assert.AreEqual(new DateTime(2022, 7, 9), result.MaxValue);
            }
        };
    }
}
