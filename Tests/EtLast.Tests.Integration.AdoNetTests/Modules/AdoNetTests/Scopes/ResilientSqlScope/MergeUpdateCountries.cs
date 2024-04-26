namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public class MergeUpdateCountries : AbstractEtlTask
{
    [ProcessParameterMustHaveValue]
    public NamedConnectionString ConnectionString { get; init; }

    public override void Execute(IFlow flow)
    {
        flow
            .CustomSqlStatement(() => new CustomSqlStatement()
            {
                Name = "CreateTable",
                ConnectionString = ConnectionString,
                SqlStatement = $"CREATE TABLE {nameof(MergeUpdateCountries)} (Id INT NOT NULL, Name VARCHAR(255), Abbreviation2 VARCHAR(2), Abbreviation3 VARCHAR(3));",
                MainTableName = nameof(MergeUpdateCountries),
            })
            .ResilientSqlScope(() => new ResilientSqlScope()
            {
                Name = "ExecuteResilientScope1",
                ConnectionString = ConnectionString,
                Tables =
                [
                    new ResilientTable()
                    {
                        TableName = nameof(MergeUpdateCountries),
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
                        TableName = nameof(MergeUpdateCountries),
                        JobCreator = UpdateRow,
                        Finalizers = builder => builder.SimpleMsSqlMerge("Id"),
                        Columns = TestData.CountryColumns,
                    },
                ],
            })
            .ExecuteProcess(() => TestHelpers.CreateReadSqlTableAndAssertExactMatch(ConnectionString, nameof(MergeUpdateCountries), new(StringComparer.InvariantCultureIgnoreCase) { ["Id"] = 1, ["Name"] = "Hungary", ["Abbreviation2"] = "HU", ["Abbreviation3"] = "HUN" },
                new(StringComparer.InvariantCultureIgnoreCase) { ["Id"] = 2, ["Name"] = "United States of America Update", ["Abbreviation2"] = "UX", ["Abbreviation3"] = "USX" })
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

    private IEnumerable<IProcess> UpdateRow(ResilientTable table)
    {
        var data = TestData.CountryData.Skip(1).Take(1).ToArray();
        data[0][1] = "United States of America Update";
        data[0][2] = "UX";
        data[0][3] = "USX";

        yield return SequenceBuilder.Fluent
            .ReadFrom(new RowCreator()
            {
                Name = "CreateUpdatedRow",
                Columns = TestData.CountryColumns,
                InputRows = [.. data]
            })
            .WriteToMsSqlResilient(new ResilientWriteToMsSqlMutator()
            {
                Name = "WriteUpdatedRow",
                ConnectionString = ConnectionString,
                TableName = table.TempTableName,
                Columns = table.Columns.ToDictionary(c => c, x => ConnectionString.Escape(x)),
            })
            .Build();
    }
}