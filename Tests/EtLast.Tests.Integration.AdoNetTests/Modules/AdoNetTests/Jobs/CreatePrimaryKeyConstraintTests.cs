namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public class CreatePrimaryKeyConstraintTests : AbstractEtlTask
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

    public override IEnumerable<IProcess> CreateJobs()
    {
        yield return new CustomSqlStatement(Context)
        {
            Name = "Create table",
            ConnectionString = ConnectionString,
            SqlStatement = $"CREATE TABLE {nameof(CreatePrimaryKeyConstraintTests)} (Id INT NOT NULL, DateTimeValue DATETIME2);" +
                    $"INSERT INTO {nameof(CreatePrimaryKeyConstraintTests)} (Id, DateTimeValue) VALUES (1, '2022.07.08');" +
                    $"INSERT INTO {nameof(CreatePrimaryKeyConstraintTests)} (Id, DateTimeValue) VALUES (2, '2022.07.09');",
        };

        yield return new CustomJob(Context)
        {
            Name = "Check no primary key",
            Action = job =>
            {
                var countOfPrimaryKeys = new GetTableRecordCount(Context)
                {
                    Name = "Read primary key(s) (1)",
                    ConnectionString = ConnectionString,
                    TableName = "INFORMATION_SCHEMA.TABLE_CONSTRAINTS",
                    CustomWhereClause = @$"TABLE_NAME = '{nameof(CreatePrimaryKeyConstraintTests)}'
                                AND CONSTRAINT_SCHEMA = 'dbo'
                                AND CONSTRAINT_CATALOG = '{DatabaseName}'
                                AND CONSTRAINT_TYPE = 'PRIMARY KEY'",
                }.ExecuteWithResult(job);

                Assert.AreEqual(0, countOfPrimaryKeys);
            }
        };

        yield return new CustomJob(Context)
        {
            Name = "Check primary key exist",
            Action = job =>
            {
                new CreatePrimaryKeyConstraint(Context)
                {
                    Name = "Create primary key",
                    ConnectionString = ConnectionString,
                    TableName = ConnectionString.Escape(nameof(CreatePrimaryKeyConstraintTests)),
                    ConstraintName = "PK_" + nameof(CreatePrimaryKeyConstraintTests),
                    Columns = new[] { "Id" }
                }.Execute(job);

                var countOfPrimaryKeys = new GetTableRecordCount(Context)
                {
                    Name = "Read primary key(s)(2)",
                    ConnectionString = ConnectionString,
                    TableName = "INFORMATION_SCHEMA.TABLE_CONSTRAINTS",
                    CustomWhereClause = @$"TABLE_NAME = '{nameof(CreatePrimaryKeyConstraintTests)}'
                                AND CONSTRAINT_SCHEMA = 'dbo'
                                AND CONSTRAINT_CATALOG = '{DatabaseName}'
                                AND CONSTRAINT_TYPE = 'PRIMARY KEY'",
                }.ExecuteWithResult(job);

                Assert.AreEqual(1, countOfPrimaryKeys);
            }
        };
    }
}
