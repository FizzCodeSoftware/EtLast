namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public class MergeUpdateCountries : AbstractEtlTask
{
    public NamedConnectionString ConnectionString { get; init; }

    public override void ValidateParameters()
    {
        if (ConnectionString == null)
            throw new ProcessParameterNullException(this, nameof(ConnectionString));
    }

    public override IEnumerable<IProcess> CreateJobs()
    {
        yield return new CustomSqlStatement(Context)
        {
            Name = "CreateTable",
            ConnectionString = ConnectionString,
            SqlStatement = $"CREATE TABLE {nameof(MergeUpdateCountries)} (Id INT NOT NULL, Name VARCHAR(255), Abbreviation2 VARCHAR(2), Abbreviation3 VARCHAR(3));"
        };

        yield return new ResilientSqlScope(Context)
        {
            Name = "ExecuteResilientScope1",
            ConnectionString = ConnectionString,
            Tables = new()
            {
                new ResilientTable()
                {
                    TableName = nameof(MergeUpdateCountries),
                    JobCreator = table => LoadFirstTwoRows(table),
                    Finalizers = builder => builder.SimpleMsSqlMerge("Id"),
                    Columns = TestData.CountryColumns,
                },
            },
        };

        yield return new ResilientSqlScope(Context)
        {
            Name = "ExecuteResilientScope2",
            ConnectionString = ConnectionString,
            Tables = new()
            {
                new ResilientTable()
                {
                    TableName = nameof(MergeUpdateCountries),
                    JobCreator = table => UpdateRow(table),
                    Finalizers = builder => builder.SimpleMsSqlMerge("Id"),
                    Columns = TestData.CountryColumns,
                },
            },
        };

        yield return TestHelpers.CreateReadSqlTableAndAssertExactMacth(this, ConnectionString, nameof(MergeUpdateCountries),
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 1, ["Name"] = "Hungary", ["Abbreviation2"] = "HU", ["Abbreviation3"] = "HUN" },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 2, ["Name"] = "United States of America Update", ["Abbreviation2"] = "UX", ["Abbreviation3"] = "USX" }
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

    private IEnumerable<IProcess> UpdateRow(ResilientTable table)
    {
        var data = TestData.CountryData.Skip(1).Take(1).ToArray();
        data[0][1] = "United States of America Update";
        data[0][2] = "UX";
        data[0][3] = "USX";

        yield return SequenceBuilder.Fluent
            .ReadFrom(new RowCreator(Context)
            {
                Name = "CreateUpdatedRow",
                Columns = TestData.CountryColumns,
                InputRows = data.ToList()
            })
            .WriteToMsSqlResilient(new ResilientWriteToMsSqlMutator(Context)
            {
                Name = "WriteUpdatedRow",
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