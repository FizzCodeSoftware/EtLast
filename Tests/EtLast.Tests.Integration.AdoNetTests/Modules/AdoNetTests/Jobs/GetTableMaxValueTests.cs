﻿namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public class GetTableMaxValueTests : AbstractEtlTask
{
    public NamedConnectionString ConnectionString { get; init; }

    public override void ValidateParameters()
    {
        if (ConnectionString == null)
            throw new ProcessParameterNullException(this, nameof(ConnectionString));
    }

    public override IEnumerable<IProcess> CreateJobs(IProcess caller)
    {
        yield return new CustomSqlStatement(Context)
        {
            Name = "CreateTableAndInsertContent",
            ConnectionString = ConnectionString,
            SqlStatement = $"CREATE TABLE {nameof(GetTableMaxValueTests)} (Id INT NOT NULL, DateTimeValue DATETIME2);" +
                    $"INSERT INTO {nameof(GetTableMaxValueTests)} (Id, DateTimeValue) VALUES (1, '2022.07.08');" +
                    $"INSERT INTO {nameof(GetTableMaxValueTests)} (Id, DateTimeValue) VALUES (1, '2022.07.09');",
        };

        yield return new CustomJob(Context)
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
                }.ExecuteWithResult(job);

                Assert.AreEqual(new DateTime(2022, 7, 9), result.MaxValue);
            }
        };
    }
}
