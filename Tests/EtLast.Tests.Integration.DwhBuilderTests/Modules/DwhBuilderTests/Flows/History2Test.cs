namespace FizzCode.EtLast.Tests.Integration.Modules.DwhBuilderTests;

public class History2Test : AbstractEtlFlow
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

    public override void Execute()
    {
        var databaseDeclaration = new TestDwhDefinition();
        databaseDeclaration.GetTable("dbo", "People").HasHistoryTable();
        databaseDeclaration.GetTable("sec", "Pet").EtlRunInfoDisabled();

        var configuration = new DwhBuilderConfiguration();
        var model = DwhDataDefinitionToRelationalModelConverter.Convert(databaseDeclaration, "dbo");

        DataDefinitionExtenderMsSql2016.Extend(databaseDeclaration, configuration);
        RelationalModelExtender.Extend(model, configuration);

        var ok = ExecuteTask(new CreateDatabase()
        {
            ConnectionString = ConnectionString,
            Definition = databaseDeclaration,
            DatabaseName = DatabaseName,
        }).Success;

        if (!ok)
            return;

        ok = ExecuteJob(CreateFirstDwhBuilder(configuration, model)).Success;

        if (!ok)
            return;

        TestFirstDwhBuilder();

        ok = ExecuteJob(CreateSecondDwhBuilder(configuration, model)).Success;

        if (!ok)
            return;

        TestSecondDwhBuilder();
    }

    private IJob CreateFirstDwhBuilder(DwhBuilderConfiguration configuration, RelationalModel model)
    {
        var builder = new MsSqlDwhBuilder(Context, "FirstDwhBuilder", Helpers.EtlRunId1)
        {
            Configuration = configuration,
            ConnectionString = ConnectionString,
            Model = model,
        };

        // BaseIsCurrentFinalizer + HasHistoryTable enabled
        builder.AddTables(model["dbo"]["People"])
            .InputIsCustomProcess(CreatePeople1)
            .SetValidFromToDefault()
            .SetValidFromToRecordTimestampIfAvailable()
            .AddMutators(PeopleMutators)
            .RemoveExistingRows(b => b
                .MatchByPrimaryKey()
                .CompareAllColumnsButValidity())
            .DisableConstraintCheck()
            .BaseIsCurrentFinalizer(b => b
                .MatchByPrimaryKey());

        // BaseIsHistoryFinalizer + AutoValidityRange
        builder.AddTables(model["dbo"]["PeopleRating"])
            .InputIsCustomProcess(CreatePeopleRating1)
            .AutoValidityRange(b => b
                .MatchBySpecificColumns("PeopleId")
                .CompareAllColumnsButValidity()
                .UsePreviousValue("Rating", "PreviousRating"))
            .DisableConstraintCheck()
            .BaseIsHistoryFinalizer(b => b
                .MatchBySpecificColumns("PeopleId"));

        // BaseIsCurrentFinalizer + HasHistoryTable disabled
        builder.AddTables(model["sec"]["Pet"])
            .InputIsCustomProcess(CreatePet1)
            .SetValidFromToDefault()
            .SetValidFromToRecordTimestampIfAvailable()
            .AddMutators(PetMutators)
            .RemoveExistingRows(b => b
                .MatchByPrimaryKey()
                .CompareAllColumnsButValidity())
            .DisableConstraintCheck()
            .BaseIsCurrentFinalizer(b => b
                .MatchByPrimaryKey());

        return builder.Build();
    }

    private void TestFirstDwhBuilder()
    {
        var result = Helpers.ReadRows(this, ConnectionString, "dbo", "People");
        Assert.AreEqual(5, result.Count);
        Assert.That.ExactMatch(result, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 0, ["Name"] = "A", ["FavoritePetId"] = 2, ["LastChangedOn"] = new DateTime(2000, 1, 1, 1, 1, 1, 0), ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(2000, 1, 1, 1, 1, 1, 0), new TimeSpan(0, 0, 0, 0, 0)) },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 1, ["Name"] = "B", ["LastChangedOn"] = new DateTime(2000, 1, 1, 1, 1, 1, 0), ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(2000, 1, 1, 1, 1, 1, 0), new TimeSpan(0, 0, 0, 0, 0)) },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 2, ["Name"] = "C", ["FavoritePetId"] = 3, ["LastChangedOn"] = new DateTime(2000, 1, 1, 1, 1, 1, 0), ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(2000, 1, 1, 1, 1, 1, 0), new TimeSpan(0, 0, 0, 0, 0)) },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 3, ["Name"] = "D", ["LastChangedOn"] = new DateTime(2000, 1, 1, 1, 1, 1, 0), ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(2000, 1, 1, 1, 1, 1, 0), new TimeSpan(0, 0, 0, 0, 0)) },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 4, ["Name"] = "E", ["LastChangedOn"] = new DateTime(2000, 1, 1, 1, 1, 1, 0), ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(2000, 1, 1, 1, 1, 1, 0), new TimeSpan(0, 0, 0, 0, 0)) } });

        result = Helpers.ReadRows(this, ConnectionString, "dbo", "People_hist");
        Assert.AreEqual(5, result.Count);
        Assert.That.ExactMatch(result, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new CaseInsensitiveStringKeyDictionary<object>() { ["People_histID"] = 1, ["Id"] = 0, ["Name"] = "A", ["FavoritePetId"] = 2, ["LastChangedOn"] = new DateTime(2000, 1, 1, 1, 1, 1, 0), ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(2000, 1, 1, 1, 1, 1, 0), new TimeSpan(0, 0, 0, 0, 0)), ["_ValidTo"] = new DateTimeOffset(new DateTime(2500, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)) },
            new CaseInsensitiveStringKeyDictionary<object>() { ["People_histID"] = 2, ["Id"] = 1, ["Name"] = "B", ["LastChangedOn"] = new DateTime(2000, 1, 1, 1, 1, 1, 0), ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(2000, 1, 1, 1, 1, 1, 0), new TimeSpan(0, 0, 0, 0, 0)), ["_ValidTo"] = new DateTimeOffset(new DateTime(2500, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)) },
            new CaseInsensitiveStringKeyDictionary<object>() { ["People_histID"] = 3, ["Id"] = 2, ["Name"] = "C", ["FavoritePetId"] = 3, ["LastChangedOn"] = new DateTime(2000, 1, 1, 1, 1, 1, 0), ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(2000, 1, 1, 1, 1, 1, 0), new TimeSpan(0, 0, 0, 0, 0)), ["_ValidTo"] = new DateTimeOffset(new DateTime(2500, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)) },
            new CaseInsensitiveStringKeyDictionary<object>() { ["People_histID"] = 4, ["Id"] = 3, ["Name"] = "D", ["LastChangedOn"] = new DateTime(2000, 1, 1, 1, 1, 1, 0), ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(2000, 1, 1, 1, 1, 1, 0), new TimeSpan(0, 0, 0, 0, 0)), ["_ValidTo"] = new DateTimeOffset(new DateTime(2500, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)) },
            new CaseInsensitiveStringKeyDictionary<object>() { ["People_histID"] = 5, ["Id"] = 4, ["Name"] = "E", ["LastChangedOn"] = new DateTime(2000, 1, 1, 1, 1, 1, 0), ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(2000, 1, 1, 1, 1, 1, 0), new TimeSpan(0, 0, 0, 0, 0)), ["_ValidTo"] = new DateTimeOffset(new DateTime(2500, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)) } });

        result = Helpers.ReadRows(this, ConnectionString, "dbo", "PeopleRating");
        Assert.AreEqual(4, result.Count);
        Assert.That.ExactMatch(result, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 1, ["PeopleId"] = 0, ["Rating"] = 7, ["_ValidFrom"] = new DateTimeOffset(new DateTime(1900, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)), ["_ValidTo"] = new DateTimeOffset(new DateTime(2500, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)), ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0) },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 2, ["PeopleId"] = 1, ["Rating"] = 4, ["_ValidFrom"] = new DateTimeOffset(new DateTime(1900, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)), ["_ValidTo"] = new DateTimeOffset(new DateTime(2500, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)), ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0) },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 3, ["PeopleId"] = 2, ["Rating"] = 3, ["_ValidFrom"] = new DateTimeOffset(new DateTime(1900, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)), ["_ValidTo"] = new DateTimeOffset(new DateTime(2500, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)), ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0) },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 4, ["PeopleId"] = 4, ["Rating"] = 9, ["_ValidFrom"] = new DateTimeOffset(new DateTime(1900, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)), ["_ValidTo"] = new DateTimeOffset(new DateTime(2500, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)), ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0) } });

        result = Helpers.ReadRows(this, ConnectionString, "sec", "Pet");
        Assert.AreEqual(3, result.Count);
        Assert.That.ExactMatch(result, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 1, ["Name"] = "pet#1", ["OwnerPeopleId"] = 0, ["LastChangedOn"] = new DateTime(2000, 1, 1, 1, 1, 1, 0) },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 2, ["Name"] = "pet#2", ["OwnerPeopleId"] = 0, ["LastChangedOn"] = new DateTime(2000, 1, 1, 1, 1, 1, 0) },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 3, ["Name"] = "pet#3", ["OwnerPeopleId"] = 2, ["LastChangedOn"] = new DateTime(2000, 1, 1, 1, 1, 1, 0) } });

        result = Helpers.ReadRows(this, ConnectionString, "dbo", "_temp_People");
        Assert.AreEqual(5, result.Count);

        result = Helpers.ReadRows(this, ConnectionString, "dbo", "_temp_PeopleRating");
        Assert.AreEqual(4, result.Count);

        result = Helpers.ReadRows(this, ConnectionString, "sec", "_temp_Pet");
        Assert.AreEqual(3, result.Count);
    }

    private IJob CreateSecondDwhBuilder(DwhBuilderConfiguration configuration, RelationalModel model)
    {
        var builder = new MsSqlDwhBuilder(Context, "SecondDwhBuilder", Helpers.EtlRunId2)
        {
            Configuration = configuration,
            ConnectionString = ConnectionString,
            Model = model,
        };

        builder.AddTables(model["dbo"]["People"])
            .InputIsCustomProcess(CreatePeople2)
            .SetValidFromToDefault()
            .SetValidFromToRecordTimestampIfAvailable()
            .AddMutators(PeopleMutators)
            .RemoveExistingRows(b => b
                .MatchByPrimaryKey()
                .CompareAllColumnsButValidity())
            .DisableConstraintCheck()
            .BaseIsCurrentFinalizer(b => b
                .MatchByPrimaryKey());

        builder.AddTables(model["dbo"]["PeopleRating"])
            .InputIsCustomProcess(CreatePeopleRating2)
            .AutoValidityRange(b => b
                .MatchBySpecificColumns("PeopleId")
                .CompareAllColumnsButValidity()
                .UsePreviousValue("Rating", "PreviousRating"))
            .DisableConstraintCheck()
            .BaseIsHistoryFinalizer(b => b
                .MatchBySpecificColumns("PeopleId"));

        builder.AddTables(model["sec"]["Pet"])
            .InputIsCustomProcess(CreatePet2)
            .SetValidFromToDefault()
            .SetValidFromToRecordTimestampIfAvailable()
            .AddMutators(PetMutators)
            .RemoveExistingRows(b => b
                .MatchByPrimaryKey()
                .CompareAllColumnsButValidity())
            .DisableConstraintCheck()
            .BaseIsCurrentFinalizer(b => b
                .MatchByPrimaryKey());

        return builder.Build();
    }

    private void TestSecondDwhBuilder()
    {
        var result = Helpers.ReadRows(this, ConnectionString, "dbo", "People");
        Assert.AreEqual(5, result.Count);
        Assert.That.ExactMatch(result, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 0, ["Name"] = "A", ["FavoritePetId"] = 2, ["LastChangedOn"] = new DateTime(2000, 1, 1, 1, 1, 1, 0), ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(2000, 1, 1, 1, 1, 1, 0), new TimeSpan(0, 0, 0, 0, 0)) },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 1, ["Name"] = "Bx", ["LastChangedOn"] = new DateTime(2010, 1, 1, 1, 1, 1, 0), ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2022, 2, 2, 2, 2, 2, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(2010, 1, 1, 1, 1, 1, 0), new TimeSpan(0, 0, 0, 0, 0)) },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 2, ["Name"] = "C", ["FavoritePetId"] = 3, ["LastChangedOn"] = new DateTime(2000, 1, 1, 1, 1, 1, 0), ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(2000, 1, 1, 1, 1, 1, 0), new TimeSpan(0, 0, 0, 0, 0)) },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 3, ["Name"] = "Dx", ["LastChangedOn"] = new DateTime(2010, 1, 1, 1, 1, 1, 0), ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2022, 2, 2, 2, 2, 2, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(2010, 1, 1, 1, 1, 1, 0), new TimeSpan(0, 0, 0, 0, 0)) },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 4, ["Name"] = "E", ["LastChangedOn"] = new DateTime(2000, 1, 1, 1, 1, 1, 0), ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(2000, 1, 1, 1, 1, 1, 0), new TimeSpan(0, 0, 0, 0, 0)) } });

        result = Helpers.ReadRows(this, ConnectionString, "dbo", "People_hist");
        Assert.AreEqual(7, result.Count);
        Assert.That.ExactMatch(result, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new CaseInsensitiveStringKeyDictionary<object>() { ["People_histID"] = 1, ["Id"] = 0, ["Name"] = "A", ["FavoritePetId"] = 2, ["LastChangedOn"] = new DateTime(2000, 1, 1, 1, 1, 1, 0), ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(2000, 1, 1, 1, 1, 1, 0), new TimeSpan(0, 0, 0, 0, 0)), ["_ValidTo"] = new DateTimeOffset(new DateTime(2500, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)) },
            new CaseInsensitiveStringKeyDictionary<object>() { ["People_histID"] = 2, ["Id"] = 1, ["Name"] = "B", ["LastChangedOn"] = new DateTime(2000, 1, 1, 1, 1, 1, 0), ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2022, 2, 2, 2, 2, 2, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunTo"] = new DateTime(2022, 2, 2, 2, 2, 2, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(2000, 1, 1, 1, 1, 1, 0), new TimeSpan(0, 0, 0, 0, 0)), ["_ValidTo"] = new DateTimeOffset(new DateTime(2010, 1, 1, 1, 1, 1, 0), new TimeSpan(0, 0, 0, 0, 0)) },
            new CaseInsensitiveStringKeyDictionary<object>() { ["People_histID"] = 3, ["Id"] = 2, ["Name"] = "C", ["FavoritePetId"] = 3, ["LastChangedOn"] = new DateTime(2000, 1, 1, 1, 1, 1, 0), ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(2000, 1, 1, 1, 1, 1, 0), new TimeSpan(0, 0, 0, 0, 0)), ["_ValidTo"] = new DateTimeOffset(new DateTime(2500, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)) },
            new CaseInsensitiveStringKeyDictionary<object>() { ["People_histID"] = 4, ["Id"] = 3, ["Name"] = "D", ["LastChangedOn"] = new DateTime(2000, 1, 1, 1, 1, 1, 0), ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2022, 2, 2, 2, 2, 2, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunTo"] = new DateTime(2022, 2, 2, 2, 2, 2, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(2000, 1, 1, 1, 1, 1, 0), new TimeSpan(0, 0, 0, 0, 0)), ["_ValidTo"] = new DateTimeOffset(new DateTime(2010, 1, 1, 1, 1, 1, 0), new TimeSpan(0, 0, 0, 0, 0)) },
            new CaseInsensitiveStringKeyDictionary<object>() { ["People_histID"] = 5, ["Id"] = 4, ["Name"] = "E", ["LastChangedOn"] = new DateTime(2000, 1, 1, 1, 1, 1, 0), ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(2000, 1, 1, 1, 1, 1, 0), new TimeSpan(0, 0, 0, 0, 0)), ["_ValidTo"] = new DateTimeOffset(new DateTime(2500, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)) },
            new CaseInsensitiveStringKeyDictionary<object>() { ["People_histID"] = 6, ["Id"] = 1, ["Name"] = "Bx", ["LastChangedOn"] = new DateTime(2010, 1, 1, 1, 1, 1, 0), ["EtlRunInsert"] = new DateTime(2022, 2, 2, 2, 2, 2, 0), ["EtlRunUpdate"] = new DateTime(2022, 2, 2, 2, 2, 2, 0), ["EtlRunFrom"] = new DateTime(2022, 2, 2, 2, 2, 2, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(2010, 1, 1, 1, 1, 1, 0), new TimeSpan(0, 0, 0, 0, 0)), ["_ValidTo"] = new DateTimeOffset(new DateTime(2500, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)) },
            new CaseInsensitiveStringKeyDictionary<object>() { ["People_histID"] = 7, ["Id"] = 3, ["Name"] = "Dx", ["LastChangedOn"] = new DateTime(2010, 1, 1, 1, 1, 1, 0), ["EtlRunInsert"] = new DateTime(2022, 2, 2, 2, 2, 2, 0), ["EtlRunUpdate"] = new DateTime(2022, 2, 2, 2, 2, 2, 0), ["EtlRunFrom"] = new DateTime(2022, 2, 2, 2, 2, 2, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(2010, 1, 1, 1, 1, 1, 0), new TimeSpan(0, 0, 0, 0, 0)), ["_ValidTo"] = new DateTimeOffset(new DateTime(2500, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)) } });

        result = Helpers.ReadRows(this, ConnectionString, "dbo", "PeopleRating");
        Assert.AreEqual(6, result.Count);
        Assert.That.ExactMatch(result, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 1, ["PeopleId"] = 0, ["Rating"] = 7, ["_ValidFrom"] = new DateTimeOffset(new DateTime(1900, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)), ["_ValidTo"] = new DateTimeOffset(new DateTime(2022, 2, 2, 2, 2, 2, 0), new TimeSpan(0, 0, 0, 0, 0)), ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2022, 2, 2, 2, 2, 2, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunTo"] = new DateTime(2022, 2, 2, 2, 2, 2, 0) },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 2, ["PeopleId"] = 1, ["Rating"] = 4, ["_ValidFrom"] = new DateTimeOffset(new DateTime(1900, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)), ["_ValidTo"] = new DateTimeOffset(new DateTime(2500, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)), ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0) },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 3, ["PeopleId"] = 2, ["Rating"] = 3, ["_ValidFrom"] = new DateTimeOffset(new DateTime(1900, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)), ["_ValidTo"] = new DateTimeOffset(new DateTime(2500, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)), ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0) },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 4, ["PeopleId"] = 4, ["Rating"] = 9, ["_ValidFrom"] = new DateTimeOffset(new DateTime(1900, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)), ["_ValidTo"] = new DateTimeOffset(new DateTime(2500, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)), ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0) },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 5, ["PeopleId"] = 0, ["Rating"] = 29, ["PreviousRating"] = 7, ["_ValidFrom"] = new DateTimeOffset(new DateTime(2022, 2, 2, 2, 2, 2, 0), new TimeSpan(0, 0, 0, 0, 0)), ["_ValidTo"] = new DateTimeOffset(new DateTime(2500, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)), ["EtlRunInsert"] = new DateTime(2022, 2, 2, 2, 2, 2, 0), ["EtlRunUpdate"] = new DateTime(2022, 2, 2, 2, 2, 2, 0), ["EtlRunFrom"] = new DateTime(2022, 2, 2, 2, 2, 2, 0) },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 6, ["PeopleId"] = 3, ["Rating"] = 23, ["_ValidFrom"] = new DateTimeOffset(new DateTime(1900, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)), ["_ValidTo"] = new DateTimeOffset(new DateTime(2500, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)), ["EtlRunInsert"] = new DateTime(2022, 2, 2, 2, 2, 2, 0), ["EtlRunUpdate"] = new DateTime(2022, 2, 2, 2, 2, 2, 0), ["EtlRunFrom"] = new DateTime(2022, 2, 2, 2, 2, 2, 0) } });

        result = Helpers.ReadRows(this, ConnectionString, "sec", "Pet");
        Assert.AreEqual(4, result.Count);
        Assert.That.ExactMatch(result, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 1, ["Name"] = "pet#1", ["OwnerPeopleId"] = 0, ["LastChangedOn"] = new DateTime(2000, 1, 1, 1, 1, 1, 0) },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 2, ["Name"] = "pet#2x", ["OwnerPeopleId"] = 0, ["LastChangedOn"] = new DateTime(2010, 1, 1, 1, 1, 1, 0) },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 3, ["Name"] = "pet#3", ["OwnerPeopleId"] = 2, ["LastChangedOn"] = new DateTime(2000, 1, 1, 1, 1, 1, 0) },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 4, ["Name"] = "pet#4x", ["OwnerPeopleId"] = 0, ["LastChangedOn"] = new DateTime(2010, 1, 1, 1, 1, 1, 0) } });

        result = Helpers.ReadRows(this, ConnectionString, "dbo", "_temp_People");
        Assert.AreEqual(2, result.Count);

        result = Helpers.ReadRows(this, ConnectionString, "dbo", "_temp_PeopleRating");
        Assert.AreEqual(2, result.Count);

        result = Helpers.ReadRows(this, ConnectionString, "sec", "_temp_Pet");
        Assert.AreEqual(2, result.Count);
    }

    public static ISequence CreatePeople1(DwhTableBuilder tableBuilder, DateTimeOffset? maxRecordTimestamp)
    {
        return new RowCreator(tableBuilder.ResilientTable.Scope.Context)
        {
            Columns = new[] { "Id", "Name", "FavoritePetId", "LastChangedOn" },
            InputRows = new List<object[]>()
            {
                new object[] { 0, "A", 2, new DateTime(2000, 1, 1, 1, 1, 1, DateTimeKind.Utc) },
                new object[] { 1, "B", null, new DateTime(2000, 1, 1, 1, 1, 1, DateTimeKind.Utc) },
                new object[] { 2, "C", 3, new DateTime(2000, 1, 1, 1, 1, 1, DateTimeKind.Utc) },
                new object[] { 3, "D", null, new DateTime(2000, 1, 1, 1, 1, 1, DateTimeKind.Utc) },
                new object[] { 4, "E", null, new DateTime(2000, 1, 1, 1, 1, 1, DateTimeKind.Utc) },
                new object[] { 5, "F", -1, new DateTime(2000, 1, 1, 1, 1, 1, DateTimeKind.Utc) },
            },
        };
    }

    public static ISequence CreatePeople2(DwhTableBuilder tableBuilder, DateTimeOffset? maxRecordTimestamp)
    {
        return new RowCreator(tableBuilder.ResilientTable.Scope.Context)
        {
            Columns = new[] { "Id", "Name", "FavoritePetId", "LastChangedOn" },
            InputRows = new List<object[]>()
            {
                new object[] { 0, "A", 2, new DateTime(2010, 1, 1, 1, 1, 1, DateTimeKind.Utc) },
                new object[] { 1, "Bx", null, new DateTime(2010, 1, 1, 1, 1, 1, DateTimeKind.Utc) },
                new object[] { 2, "C", 3, new DateTime(2000, 1, 1, 1, 1, 1, DateTimeKind.Utc) },
                new object[] { 3, "Dx", null, new DateTime(2010, 1, 1, 1, 1, 1, DateTimeKind.Utc) },
                new object[] { 4, "E", null, new DateTime(2000, 1, 1, 1, 1, 1, DateTimeKind.Utc) },
                new object[] { 5, "Fx", -1, new DateTime(2010, 1, 1, 1, 1, 1, DateTimeKind.Utc) },
            },
        };
    }

    public static ISequence CreatePeopleRating1(DwhTableBuilder tableBuilder, DateTimeOffset? maxRecordTimestamp)
    {
        return new RowCreator(tableBuilder.ResilientTable.Scope.Context)
        {
            Columns = new[] { "PeopleId", "Rating" },
            InputRows = new List<object[]>()
            {
                new object[] { 0, 7 },
                new object[] { 1, 4 },
                new object[] { 2, 3 },
                new object[] { 4, 9 },
            },
        };
    }

    public static ISequence CreatePeopleRating2(DwhTableBuilder tableBuilder, DateTimeOffset? maxRecordTimestamp)
    {
        return new RowCreator(tableBuilder.ResilientTable.Scope.Context)
        {
            Columns = new[] { "PeopleId", "Rating" },
            InputRows = new List<object[]>()
            {
                new object[] { 0, 29 },
                new object[] { 3, 23 },
            },
        };
    }

    public static ISequence CreatePet1(DwhTableBuilder tableBuilder, DateTimeOffset? maxRecordTimestamp)
    {
        return new RowCreator(tableBuilder.ResilientTable.Scope.Context)
        {
            Columns = new[] { "Id", "Name", "OwnerPeopleId", "LastChangedOn" },
            InputRows = new List<object[]>()
            {
                new object[] { 1, "pet#1", 0, new DateTime(2000, 1, 1, 1, 1, 1, DateTimeKind.Utc) },
                new object[] { 2, "pet#2", 0, new DateTime(2000, 1, 1, 1, 1, 1, DateTimeKind.Utc) },
                new object[] { 3, "pet#3", 2, new DateTime(2000, 1, 1, 1, 1, 1, DateTimeKind.Utc) },
                new object[] { 4, "pet#4", null, new DateTime(2000, 1, 1, 1, 1, 1, DateTimeKind.Utc) },
                new object[] { 5, "pet#5", -1, new DateTime(2000, 1, 1, 1, 1, 1, DateTimeKind.Utc) },
            },
        };
    }

    public static ISequence CreatePet2(DwhTableBuilder tableBuilder, DateTimeOffset? maxRecordTimestamp)
    {
        return new RowCreator(tableBuilder.ResilientTable.Scope.Context)
        {
            Columns = new[] { "Id", "Name", "OwnerPeopleId", "LastChangedOn" },
            InputRows = new List<object[]>()
            {
                new object[] { 1, "pet#1", 0, new DateTime(2000, 1, 1, 1, 1, 1, DateTimeKind.Utc) },
                new object[] { 2, "pet#2x", 0, new DateTime(2010, 1, 1, 1, 1, 1, DateTimeKind.Utc) },
                new object[] { 3, "pet#3", 2, new DateTime(2000, 1, 1, 1, 1, 1, DateTimeKind.Utc) },
                new object[] { 4, "pet#4x", 0, new DateTime(2010, 1, 1, 1, 1, 1, DateTimeKind.Utc) },
                new object[] { 5, "pet#5", -1, new DateTime(2000, 1, 1, 1, 1, 1, DateTimeKind.Utc) },
            },
        };
    }

    private static IEnumerable<IMutator> PeopleMutators(DwhTableBuilder tableBuilder)
    {
        yield return new CustomMutator(tableBuilder.ResilientTable.Scope.Context)
        {
            Action = row =>
            {
                var fk = row.GetAs<int?>("FavoritePetId");
                return fk == null || fk.Value >= 0;
            },
        };
    }

    private static IEnumerable<IMutator> PetMutators(DwhTableBuilder tableBuilder)
    {
        yield return new CustomMutator(tableBuilder.ResilientTable.Scope.Context)
        {
            Action = row =>
            {
                var fk = row.GetAs<int?>("OwnerPeopleId");
                return fk >= 0;
            },
        };
    }
}
