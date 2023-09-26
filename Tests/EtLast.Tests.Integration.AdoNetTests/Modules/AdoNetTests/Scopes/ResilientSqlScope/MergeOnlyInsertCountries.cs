namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public class MergeOnlyInsertCountries : AbstractEtlTask
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
            .CustomSqlStatement(() => new CustomSqlStatement(Context)
            {
                Name = "CreateTable",
                ConnectionString = ConnectionString,
                SqlStatement = $"CREATE TABLE {nameof(MergeOnlyInsertCountries)} (Id INT NOT NULL, Name VARCHAR(255), Abbreviation2 VARCHAR(2), Abbreviation3 VARCHAR(3));",
                MainTableName = nameof(MergeOnlyInsertCountries),
            })
            .ResilientSqlScope(() => new ResilientSqlScope(Context)
            {
                Name = "ExecuteResilientScope1",
                ConnectionString = ConnectionString,
                Tables = new()
                {
                    new ResilientTable()
                    {
                        TableName = nameof(MergeOnlyInsertCountries),
                        JobCreator = LoadFirstTwoRows,
                        Finalizers = builder => builder.SimpleMsSqlMerge("Id"),
                        Columns = TestData.CountryColumns,
                    },
                },
            })
            .ResilientSqlScope(() => new ResilientSqlScope(Context)
            {
                Name = "ExecuteResilientScope2",
                ConnectionString = ConnectionString,
                Tables = new()
                {
                    new ResilientTable()
                    {
                        TableName = nameof(MergeOnlyInsertCountries),
                        JobCreator = LoadSecondTwoRows,
                        Finalizers = builder => builder.SimpleMsSqlMerge("Id"),
                        Columns = TestData.CountryColumns,
                    },
                },
            })
            .ExecuteProcess(() => TestHelpers.CreateReadSqlTableAndAssertExactMacth(this, ConnectionString, nameof(MergeOnlyInsertCountries),
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 1, ["Name"] = "Hungary", ["Abbreviation2"] = "HU", ["Abbreviation3"] = "HUN" },
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 2, ["Name"] = "United States of America", ["Abbreviation2"] = "US", ["Abbreviation3"] = "USA" },
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 3, ["Name"] = "Spain", ["Abbreviation2"] = "ES", ["Abbreviation3"] = "ESP" },
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 4, ["Name"] = "Mexico", ["Abbreviation2"] = "MX", ["Abbreviation3"] = "MEX" })
            );
    }

    private IEnumerable<IProcess> LoadFirstTwoRows(ResilientTable table)
    {
        yield return SequenceBuilder.Fluent
            .ReadFrom(new RowCreator(Context)
            {
                Name = "CreateFirstTwoRows",
                Columns = TestData.CountryColumns,
                InputRows = TestData.CountryData.Take(2).ToList()
            })
            .WriteToMsSqlResilient(new ResilientWriteToMsSqlMutator(Context)
            {
                Name = "WriteFirstTwoRows",
                ConnectionString = ConnectionString,
                TableDefinition = new DbTableDefinition()
                {
                    TableName = table.TempTableName,
                    Columns = table.Columns.ToDictionary(c => c, x => ConnectionString.Escape(x)),
                }
            })
            .Build();
    }

    private IEnumerable<IProcess> LoadSecondTwoRows(ResilientTable table)
    {
        yield return SequenceBuilder.Fluent
            .ReadFrom(new RowCreator(Context)
            {
                Name = "CreateSecondTwoRows",
                Columns = TestData.CountryColumns,
                InputRows = TestData.CountryData.Skip(2).ToList()
            })
            .WriteToMsSqlResilient(new ResilientWriteToMsSqlMutator(Context)
            {
                Name = "WriteSecondTwoRows",
                ConnectionString = ConnectionString,
                TableDefinition = new DbTableDefinition()
                {
                    TableName = table.TempTableName,
                    Columns = table.Columns.ToDictionary(c => c, x => ConnectionString.Escape(x)),
                }
            })
            .Build();
    }
}