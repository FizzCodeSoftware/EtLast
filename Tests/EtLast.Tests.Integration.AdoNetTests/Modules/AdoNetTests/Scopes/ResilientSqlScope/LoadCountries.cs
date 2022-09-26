namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public class LoadCountries : AbstractEtlTask
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
            Name = "Create table",
            ConnectionString = ConnectionString,
            SqlStatement = $"CREATE TABLE {nameof(LoadCountries)} (Id INT NOT NULL, Name VARCHAR(255), Abbreviation2 VARCHAR(2), Abbreviation3 VARCHAR(3));"
        };

        yield return new ResilientSqlScope(Context)
        {
            Name = "Execute resilient scope",
            ConnectionString = ConnectionString,
            Tables = new()
            {
                new ResilientTable()
                {
                    TableName = nameof(LoadCountries),
                    JobCreator = table => CreateProcess(table),
                    Finalizers = builder => builder.CopyTable(),
                    Columns = TestData.CountryColumns,
                },
            },
        };

        yield return TestHelpers.CreateReadSqlTableAndAssertExactMacth(this, ConnectionString, nameof(LoadCountries),
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 1, ["Name"] = "Hungary", ["Abbreviation2"] = "HU", ["Abbreviation3"] = "HUN" },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 2, ["Name"] = "United States of America", ["Abbreviation2"] = "US", ["Abbreviation3"] = "USA" },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 3, ["Name"] = "Spain", ["Abbreviation2"] = "ES", ["Abbreviation3"] = "ESP" },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 4, ["Name"] = "Mexico", ["Abbreviation2"] = "MX", ["Abbreviation3"] = "MEX" }
            );
    }

    private IEnumerable<IProcess> CreateProcess(ResilientTable table)
    {
        yield return SequenceBuilder.Fluent
            .ReadFrom(TestData.Country(Context))
            .WriteToMsSqlResilient(new ResilientWriteToMsSqlMutator(Context)
            {
                Name = "Write content to table",
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