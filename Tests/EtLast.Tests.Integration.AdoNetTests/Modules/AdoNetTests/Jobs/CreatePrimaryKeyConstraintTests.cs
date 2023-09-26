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

    public override void Execute(IFlow flow)
    {
        flow
            .CustomSqlStatement(() => new CustomSqlStatement(Context)
            {
                Name = "CreateTable",
                ConnectionString = ConnectionString,
                SqlStatement = $"CREATE TABLE {nameof(CreatePrimaryKeyConstraintTests)} (Id INT NOT NULL, DateTimeValue DATETIME2);" +
                    $"INSERT INTO {nameof(CreatePrimaryKeyConstraintTests)} (Id, DateTimeValue) VALUES (1, '2022.07.08');" +
                    $"INSERT INTO {nameof(CreatePrimaryKeyConstraintTests)} (Id, DateTimeValue) VALUES (2, '2022.07.09');",
                MainTableName = nameof(CreatePrimaryKeyConstraintTests),
            })
            .GetTableRecordCount(out var countOfPrimaryKeys1, () => new GetTableRecordCount(Context)
            {
                Name = "ReadPrimaryKey1",
                ConnectionString = ConnectionString,
                TableName = "INFORMATION_SCHEMA.TABLE_CONSTRAINTS",
                WhereClause = @$"TABLE_NAME = '{nameof(CreatePrimaryKeyConstraintTests)}'
                        AND CONSTRAINT_SCHEMA = 'dbo'
                        AND CONSTRAINT_CATALOG = '{DatabaseName}'
                        AND CONSTRAINT_TYPE = 'PRIMARY KEY'",
            })
            .CustomJob("Test", job =>
            {
                Assert.AreEqual(0, countOfPrimaryKeys1);
            })
            .CreatePrimaryKeyConstraint(() => new CreatePrimaryKeyConstraint(Context)
            {
                Name = "CreatePrimaryKey",
                ConnectionString = ConnectionString,
                TableName = ConnectionString.Escape(nameof(CreatePrimaryKeyConstraintTests)),
                ConstraintName = "PK_" + nameof(CreatePrimaryKeyConstraintTests),
                Columns = new[] { "Id" }
            })
            .GetTableRecordCount(out var countOfPrimaryKeys2, () => new GetTableRecordCount(Context)
            {
                Name = "ReadPrimaryKey2",
                ConnectionString = ConnectionString,
                TableName = "INFORMATION_SCHEMA.TABLE_CONSTRAINTS",
                WhereClause = @$"TABLE_NAME = '{nameof(CreatePrimaryKeyConstraintTests)}'
                        AND CONSTRAINT_SCHEMA = 'dbo'
                        AND CONSTRAINT_CATALOG = '{DatabaseName}'
                        AND CONSTRAINT_TYPE = 'PRIMARY KEY'",
            })
            .CustomJob("Test", job =>
            {
                Assert.AreEqual(1, countOfPrimaryKeys2);
            });
    }
}