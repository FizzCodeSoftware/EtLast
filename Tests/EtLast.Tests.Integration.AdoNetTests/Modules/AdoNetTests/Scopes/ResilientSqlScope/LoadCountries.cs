namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public class LoadCountries : AbstractEtlTask
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
                SqlStatement = $"CREATE TABLE {nameof(LoadCountries)} (Id INT NOT NULL, Name VARCHAR(255), Abbreviation2 VARCHAR(2), Abbreviation3 VARCHAR(3));",
                MainTableName = nameof(LoadCountries),
            })
            .ResilientSqlScope(() => new ResilientSqlScope()
            {
                Name = "ExecuteResilientScope",
                ConnectionString = ConnectionString,
                Tables =
                [
                    new ResilientTable()
                    {
                        TableName = nameof(LoadCountries),
                        JobCreator = CreateProcess,
                        Finalizers = builder => builder.CopyTable(),
                        Columns = TestData.CountryColumns,
                    },
                ],
            })
            .ExecuteProcess(() => TestHelpers.CreateReadSqlTableAndAssertExactMatch(ConnectionString, nameof(LoadCountries), new(StringComparer.InvariantCultureIgnoreCase) { ["Id"] = 1, ["Name"] = "Hungary", ["Abbreviation2"] = "HU", ["Abbreviation3"] = "HUN" },
                new(StringComparer.InvariantCultureIgnoreCase) { ["Id"] = 2, ["Name"] = "United States of America", ["Abbreviation2"] = "US", ["Abbreviation3"] = "USA" },
                new(StringComparer.InvariantCultureIgnoreCase) { ["Id"] = 3, ["Name"] = "Spain", ["Abbreviation2"] = "ES", ["Abbreviation3"] = "ESP" },
                new(StringComparer.InvariantCultureIgnoreCase) { ["Id"] = 4, ["Name"] = "Mexico", ["Abbreviation2"] = "MX", ["Abbreviation3"] = "MEX" })
            );
    }

    private IEnumerable<IProcess> CreateProcess(ResilientTable table)
    {
        yield return SequenceBuilder.Fluent
            .ReadFrom(TestData.Country())
            .WriteToMsSqlResilient(new ResilientWriteToMsSqlMutator()
            {
                Name = "WriteContentToTable",
                ConnectionString = ConnectionString,
                TableName = table.TempTableName,
                Columns = table.Columns.ToDictionary(c => c, x => ConnectionString.Escape(x)),
            })
            .Build();
    }
}