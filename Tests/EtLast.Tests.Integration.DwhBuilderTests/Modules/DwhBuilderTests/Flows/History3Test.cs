namespace FizzCode.EtLast.Tests.Integration.Modules.DwhBuilderTests;

public class History3Test : AbstractEtlTask
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
        var databaseDeclaration = new TestDwhDefinition();
        databaseDeclaration.GetTable("dbo", "Company").HasHistoryTable();

        var configuration = new DwhBuilderConfiguration();
        var model = DwhDataDefinitionToRelationalModelConverter.Convert(databaseDeclaration, "dbo");

        DataDefinitionExtenderMsSql2016.Extend(databaseDeclaration, configuration);
        RelationalModelExtender.Extend(model, configuration);

        flow
            .ExecuteProcess(() => new CreateDatabase()
            {
                ConnectionString = ConnectionString,
                Definition = databaseDeclaration,
                DatabaseName = DatabaseName,
            })
            .ExecuteProcess(() => CreateFirstDwhBuilder(configuration, model))
            .CustomJob(nameof(TestFirstDwhBuilder), TestFirstDwhBuilder)
            .ExecuteProcess(() => CreateSecondDwhBuilder(configuration, model))
            .CustomJob(nameof(TestSecondDwhBuilder), TestSecondDwhBuilder);
    }

    private IProcess CreateFirstDwhBuilder(DwhBuilderConfiguration configuration, RelationalModel model)
    {
        var builder = new MsSqlDwhBuilder(Context, "FirstDwhBuilder", Helpers.EtlRunId1)
        {
            Configuration = configuration,
            ConnectionString = ConnectionString,
            Model = model,
        };

        // BaseIsCurrentFinalizer + HasHistoryTable enabled
        builder.AddTables(model["dbo"]["Company"])
            .InputIsCustomProcess(CreateCompany1)
            .SetValidFromToDefault()
            .RemoveExistingRows(b => b
                .MatchByPrimaryKey()
                .CompareAllColumnsButValidity()
                .AutoValidityIfValueChanged())
            .DisableConstraintCheck()
            .BaseIsCurrentFinalizer(b => b
                .MatchByPrimaryKey());

        return builder.Build();
    }

    private void TestFirstDwhBuilder(CustomJob job)
    {
        var result = Helpers.ReadRows(this, ConnectionString, "dbo", "Company");
        Assert.AreEqual(4, result.Count);
        Assert.That.ExactMatch(result, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 1, ["Name"] = "A", ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(1900, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)) },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 2, ["Name"] = "B", ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(1900, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)) },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 3, ["Name"] = "C", ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(1900, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)) },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 4, ["Name"] = "D", ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(1900, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)) } });

        result = Helpers.ReadRows(this, ConnectionString, "dbo", "Company_hist");
        Assert.AreEqual(4, result.Count);
        Assert.That.ExactMatch(result, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new CaseInsensitiveStringKeyDictionary<object>() { ["Company_histID"] = 1, ["Id"] = 1, ["Name"] = "A", ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(1900, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)), ["_ValidTo"] = new DateTimeOffset(new DateTime(2500, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)) },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Company_histID"] = 2, ["Id"] = 2, ["Name"] = "B", ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(1900, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)), ["_ValidTo"] = new DateTimeOffset(new DateTime(2500, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)) },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Company_histID"] = 3, ["Id"] = 3, ["Name"] = "C", ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(1900, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)), ["_ValidTo"] = new DateTimeOffset(new DateTime(2500, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)) },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Company_histID"] = 4, ["Id"] = 4, ["Name"] = "D", ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(1900, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)), ["_ValidTo"] = new DateTimeOffset(new DateTime(2500, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)) } });

        result = Helpers.ReadRows(this, ConnectionString, "dbo", "_temp_Company");
        Assert.AreEqual(4, result.Count);
    }

    private IProcess CreateSecondDwhBuilder(DwhBuilderConfiguration configuration, RelationalModel model)
    {
        var builder = new MsSqlDwhBuilder(Context, "SecondDwhBuilder", Helpers.EtlRunId2)
        {
            Configuration = configuration,
            ConnectionString = ConnectionString,
            Model = model,
        };

        builder.AddTables(model["dbo"]["Company"])
            .InputIsCustomProcess(CreateCompany2)
            .SetValidFromToDefault()
            .RemoveExistingRows(b => b
                .MatchByPrimaryKey()
                .CompareAllColumnsButValidity()
                .AutoValidityIfValueChanged())
            .DisableConstraintCheck()
            .BaseIsCurrentFinalizer(b => b
                .MatchByPrimaryKey());

        return builder.Build();
    }

    private void TestSecondDwhBuilder(CustomJob job)
    {
        var result = Helpers.ReadRows(this, ConnectionString, "dbo", "Company");
        Assert.AreEqual(5, result.Count);
        Assert.That.ExactMatch(result, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 1, ["Name"] = "A", ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(1900, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)) },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 2, ["Name"] = "Bx", ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2022, 2, 2, 2, 2, 2, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(2022, 2, 2, 2, 2, 2, 0), new TimeSpan(0, 0, 0, 0, 0)) },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 3, ["Name"] = "Cx", ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2022, 2, 2, 2, 2, 2, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(2022, 2, 2, 2, 2, 2, 0), new TimeSpan(0, 0, 0, 0, 0)) },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 4, ["Name"] = "D", ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(1900, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)) },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 5, ["Name"] = "E", ["EtlRunInsert"] = new DateTime(2022, 2, 2, 2, 2, 2, 0), ["EtlRunUpdate"] = new DateTime(2022, 2, 2, 2, 2, 2, 0), ["EtlRunFrom"] = new DateTime(2022, 2, 2, 2, 2, 2, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(1900, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)) } });

        result = Helpers.ReadRows(this, ConnectionString, "dbo", "Company_hist");
        Assert.AreEqual(7, result.Count);
        Assert.That.ExactMatch(result, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new CaseInsensitiveStringKeyDictionary<object>() { ["Company_histID"] = 1, ["Id"] = 1, ["Name"] = "A", ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(1900, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)), ["_ValidTo"] = new DateTimeOffset(new DateTime(2500, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)) },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Company_histID"] = 2, ["Id"] = 2, ["Name"] = "B", ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2022, 2, 2, 2, 2, 2, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunTo"] = new DateTime(2022, 2, 2, 2, 2, 2, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(1900, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)), ["_ValidTo"] = new DateTimeOffset(new DateTime(2022, 2, 2, 2, 2, 2, 0), new TimeSpan(0, 0, 0, 0, 0)) },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Company_histID"] = 3, ["Id"] = 3, ["Name"] = "C", ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2022, 2, 2, 2, 2, 2, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunTo"] = new DateTime(2022, 2, 2, 2, 2, 2, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(1900, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)), ["_ValidTo"] = new DateTimeOffset(new DateTime(2022, 2, 2, 2, 2, 2, 0), new TimeSpan(0, 0, 0, 0, 0)) },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Company_histID"] = 4, ["Id"] = 4, ["Name"] = "D", ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(1900, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)), ["_ValidTo"] = new DateTimeOffset(new DateTime(2500, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)) },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Company_histID"] = 5, ["Id"] = 2, ["Name"] = "Bx", ["EtlRunInsert"] = new DateTime(2022, 2, 2, 2, 2, 2, 0), ["EtlRunUpdate"] = new DateTime(2022, 2, 2, 2, 2, 2, 0), ["EtlRunFrom"] = new DateTime(2022, 2, 2, 2, 2, 2, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(2022, 2, 2, 2, 2, 2, 0), new TimeSpan(0, 0, 0, 0, 0)), ["_ValidTo"] = new DateTimeOffset(new DateTime(2500, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)) },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Company_histID"] = 6, ["Id"] = 3, ["Name"] = "Cx", ["EtlRunInsert"] = new DateTime(2022, 2, 2, 2, 2, 2, 0), ["EtlRunUpdate"] = new DateTime(2022, 2, 2, 2, 2, 2, 0), ["EtlRunFrom"] = new DateTime(2022, 2, 2, 2, 2, 2, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(2022, 2, 2, 2, 2, 2, 0), new TimeSpan(0, 0, 0, 0, 0)), ["_ValidTo"] = new DateTimeOffset(new DateTime(2500, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)) },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Company_histID"] = 7, ["Id"] = 5, ["Name"] = "E", ["EtlRunInsert"] = new DateTime(2022, 2, 2, 2, 2, 2, 0), ["EtlRunUpdate"] = new DateTime(2022, 2, 2, 2, 2, 2, 0), ["EtlRunFrom"] = new DateTime(2022, 2, 2, 2, 2, 2, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(1900, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)), ["_ValidTo"] = new DateTimeOffset(new DateTime(2500, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)) } });

        result = Helpers.ReadRows(this, ConnectionString, "dbo", "_temp_Company");
        Assert.AreEqual(3, result.Count);
    }

    public static ISequence CreateCompany1(DwhTableBuilder tableBuilder, DateTimeOffset? maxRecordTimestamp)
    {
        return new RowCreator(tableBuilder.ResilientTable.Scope.Context)
        {
            Columns = new[] { "Id", "Name" },
            InputRows = new List<object[]>()
            {
                new object[] { 1, "A" },
                new object[] { 2, "B" },
                new object[] { 3, "C" },
                new object[] { 4, "D" },
            },
        };
    }

    public static ISequence CreateCompany2(DwhTableBuilder tableBuilder, DateTimeOffset? maxRecordTimestamp)
    {
        return new RowCreator(tableBuilder.ResilientTable.Scope.Context)
        {
            Columns = new[] { "Id", "Name" },
            InputRows = new List<object[]>()
            {
                new object[] { 2, "Bx" },
                new object[] { 3, "Cx" },
                new object[] { 5, "E" },
            },
        };
    }
}
