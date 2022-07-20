namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public class CreatePrimaryKeyConstraint : AbstractEtlTask
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
            SqlStatement = $"CREATE TABLE {nameof(CreatePrimaryKeyConstraint)} (Id INT NOT NULL, DateTimeValue DATETIME2);" +
                    $"INSERT INTO {nameof(CreatePrimaryKeyConstraint)} (Id, DateTimeValue) VALUES (1, '2022.07.08');" +
                    $"INSERT INTO {nameof(CreatePrimaryKeyConstraint)} (Id, DateTimeValue) VALUES (2, '2022.07.09');",
        };

        yield return new CustomJob(Context)
        {
            Name = "Check no primary key",
            Action = job =>
            {
                var countOfPrimaryKeys = new EtLast.GetTableRecordCount(Context)
                {
                    ConnectionString = ConnectionString,
                    TableName = "INFORMATION_SCHEMA.TABLE_CONSTRAINTS",
                    CustomWhereClause = @$"TABLE_NAME = '{nameof(CreatePrimaryKeyConstraint)}'
                                AND CONSTRAINT_SCHEMA = 'dbo'
                                AND CONSTRAINT_CATALOG = '{DatabaseName}'
                                AND CONSTRAINT_TYPE = 'PRIMARY KEY'",
                }.ExecuteWithResult();

                Assert.AreEqual(0, countOfPrimaryKeys);
            }
        };

        yield return new CustomJob(Context)
        {
            Name = "Check primary key exist",
            Action = job =>
            {
                new EtLast.CreatePrimaryKeyConstraint(Context)
                {
                    ConnectionString = ConnectionString,
                    TableName = ConnectionString.Escape(nameof(CreatePrimaryKeyConstraint)),
                    ConstraintName = "PK_" + nameof(CreatePrimaryKeyConstraint),
                    Columns = new[] { "Id" }
                }.Execute();


            var countOfPrimaryKeys = new EtLast.GetTableRecordCount(Context)
            {
                ConnectionString = ConnectionString,
                TableName = "INFORMATION_SCHEMA.TABLE_CONSTRAINTS",
                CustomWhereClause = @$"TABLE_NAME = '{nameof(CreatePrimaryKeyConstraint)}'
                                AND CONSTRAINT_SCHEMA = 'dbo'
                                AND CONSTRAINT_CATALOG = '{DatabaseName}'
                                AND CONSTRAINT_TYPE = 'PRIMARY KEY'",
                }.ExecuteWithResult();

                
                Assert.AreEqual(1, countOfPrimaryKeys);
            }
        };
    }
}
