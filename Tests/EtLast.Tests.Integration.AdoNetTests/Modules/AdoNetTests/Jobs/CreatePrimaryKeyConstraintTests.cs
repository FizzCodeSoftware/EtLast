namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public class CreatePrimaryKeyConstraintTests : AbstractEtlTask
{
    [ProcessParameterMustHaveValue]
    public NamedConnectionString ConnectionString { get; init; }

    [ProcessParameterMustHaveValue]
    public string DatabaseName { get; init; }

    public override void Execute(IFlow flow)
    {
        flow
            .CustomSqlStatement(() => new CustomSqlStatement()
            {
                Name = "CreateTable",
                ConnectionString = ConnectionString,
                SqlStatement = $"CREATE TABLE {nameof(CreatePrimaryKeyConstraintTests)} (Id INT NOT NULL, DateTimeValue DATETIME2);" +
                    $"INSERT INTO {nameof(CreatePrimaryKeyConstraintTests)} (Id, DateTimeValue) VALUES (1, '2022.07.08');" +
                    $"INSERT INTO {nameof(CreatePrimaryKeyConstraintTests)} (Id, DateTimeValue) VALUES (2, '2022.07.09');",
                MainTableName = nameof(CreatePrimaryKeyConstraintTests),
            })
            .GetTableRecordCount(out var countOfPrimaryKeys1, () => new GetTableRecordCount()
            {
                Name = "ReadPrimaryKey1",
                ConnectionString = ConnectionString,
                TableName = "INFORMATION_SCHEMA.TABLE_CONSTRAINTS",
                WhereClause = @$"TABLE_NAME = '{nameof(CreatePrimaryKeyConstraintTests)}'
                        AND CONSTRAINT_SCHEMA = 'dbo'
                        AND CONSTRAINT_CATALOG = '{DatabaseName}'
                        AND CONSTRAINT_TYPE = 'PRIMARY KEY'",
            })
            .CustomJob("Test", job => Assert.AreEqual(0, countOfPrimaryKeys1))
            .CreatePrimaryKeyConstraint(() => new CreatePrimaryKeyConstraint()
            {
                Name = "CreatePrimaryKey",
                ConnectionString = ConnectionString,
                TableName = ConnectionString.Escape(nameof(CreatePrimaryKeyConstraintTests)),
                ConstraintName = "PK_" + nameof(CreatePrimaryKeyConstraintTests),
                Columns = ["Id"]
            })
            .GetTableRecordCount(out var countOfPrimaryKeys2, () => new GetTableRecordCount()
            {
                Name = "ReadPrimaryKey2",
                ConnectionString = ConnectionString,
                TableName = "INFORMATION_SCHEMA.TABLE_CONSTRAINTS",
                WhereClause = @$"TABLE_NAME = '{nameof(CreatePrimaryKeyConstraintTests)}'
                        AND CONSTRAINT_SCHEMA = 'dbo'
                        AND CONSTRAINT_CATALOG = '{DatabaseName}'
                        AND CONSTRAINT_TYPE = 'PRIMARY KEY'",
            })
            .CustomJob("Test", job => Assert.AreEqual(1, countOfPrimaryKeys2));
    }
}