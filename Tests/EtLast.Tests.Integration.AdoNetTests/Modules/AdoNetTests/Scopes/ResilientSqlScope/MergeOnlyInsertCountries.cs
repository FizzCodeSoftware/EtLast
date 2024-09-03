namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public class MergeOnlyInsertCountries : AbstractEtlTask
{
    [ProcessParameterMustHaveValue]
    public MsSqlConnectionString ConnectionString { get; init; }

    public override void Execute(IFlow flow)
    {
        flow
            .CustomSqlStatement(() => new CustomSqlStatement()
            {
                Name = "CreateTable",
                ConnectionString = ConnectionString,
                SqlStatement = $"CREATE TABLE {nameof(MergeOnlyInsertCountries)} (Id INT NOT NULL, Name VARCHAR(255), Abbreviation2 VARCHAR(2), Abbreviation3 VARCHAR(3));",
                MainTableName = nameof(MergeOnlyInsertCountries),
            })
            .ResilientSqlScope(() => new ResilientSqlScope()
            {
                Name = "ExecuteResilientScope1",
                ConnectionString = ConnectionString,
                Tables =
                [
                    new ResilientTable()
                    {
                        TableName = nameof(MergeOnlyInsertCountries),
                        JobCreator = LoadFirstTwoRows,
                        Finalizers = builder => builder.SimpleMsSqlMerge("Id"),
                        Columns = TestData.CountryColumns,
                    },
                ],
            })
            .ResilientSqlScope(() => new ResilientSqlScope()
            {
                Name = "ExecuteResilientScope2",
                ConnectionString = ConnectionString,
                Tables =
                [
                    new ResilientTable()
                    {
                        TableName = nameof(MergeOnlyInsertCountries),
                        JobCreator = LoadSecondTwoRows,
                        Finalizers = builder => builder.SimpleMsSqlMerge("Id"),
                        Columns = TestData.CountryColumns,
                    },
                ],
            })
            .ExecuteProcess(() => TestHelpers.CreateReadSqlTableAndAssertExactMatch(ConnectionString, nameof(MergeOnlyInsertCountries), new(StringComparer.InvariantCultureIgnoreCase) { ["Id"] = 1, ["Name"] = "Hungary", ["Abbreviation2"] = "HU", ["Abbreviation3"] = "HUN" },
                new(StringComparer.InvariantCultureIgnoreCase) { ["Id"] = 2, ["Name"] = "United States of America", ["Abbreviation2"] = "US", ["Abbreviation3"] = "USA" },
                new(StringComparer.InvariantCultureIgnoreCase) { ["Id"] = 3, ["Name"] = "Spain", ["Abbreviation2"] = "ES", ["Abbreviation3"] = "ESP" },
                new(StringComparer.InvariantCultureIgnoreCase) { ["Id"] = 4, ["Name"] = "Mexico", ["Abbreviation2"] = "MX", ["Abbreviation3"] = "MEX" })
            );
    }

    private IEnumerable<IProcess> LoadFirstTwoRows(ResilientTable table)
    {
        yield return SequenceBuilder.Fluent
            .ReadFrom(new RowCreator()
            {
                Name = "CreateFirstTwoRows",
                Columns = TestData.CountryColumns,
                InputRows = TestData.CountryData.Take(2).ToList()
            })
            .WriteToMsSqlResilient(new ResilientWriteToMsSqlMutator()
            {
                Name = "WriteFirstTwoRows",
                ConnectionString = ConnectionString,
                TableName = table.TempTableName,
                Columns = table.Columns.ToDictionary(c => c, x => ConnectionString.Escape(x)),
            })
            .Build();
    }

    private IEnumerable<IProcess> LoadSecondTwoRows(ResilientTable table)
    {
        yield return SequenceBuilder.Fluent
            .ReadFrom(new RowCreator()
            {
                Name = "CreateSecondTwoRows",
                Columns = TestData.CountryColumns,
                InputRows = TestData.CountryData.Skip(2).ToList()
            })
            .WriteToMsSqlResilient(new ResilientWriteToMsSqlMutator()
            {
                Name = "WriteSecondTwoRows",
                ConnectionString = ConnectionString,
                TableName = table.TempTableName,
                Columns = table.Columns.ToDictionary(c => c, x => ConnectionString.Escape(x)),
            })
            .Build();
    }
}