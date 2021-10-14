namespace FizzCode.EtLast.Tests.Integration.Modules.DwhBuilderTests
{
    using System;
    using System.Collections.Generic;
    using FizzCode.EtLast;
    using FizzCode.EtLast.DwhBuilder;
    using FizzCode.EtLast.DwhBuilder.Extenders.DataDefinition;
    using FizzCode.EtLast.DwhBuilder.Extenders.DataDefinition.MsSql;
    using FizzCode.EtLast.DwhBuilder.MsSql;
    using FizzCode.LightWeight.Collections;
    using FizzCode.LightWeight.RelationalModel;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    // EtlRunInfo ON, no record timestamp
    public class EtlRunIdForDefaultValidFromTest : AbstractDwhBuilderTestPlugin
    {
        public override IEnumerable<IExecutable> CreateExecutables()
        {
            yield return new CustomAction(PluginTopic, "CustomAction")
            {
                Then = _ =>
                {
                    DatabaseDeclaration.GetTable("dbo", "Company").HasHistoryTable();

                    var configuration = new DwhBuilderConfiguration()
                    {
                        UseEtlRunIdForDefaultValidFrom = true,
                    };

                    var model = DwhDataDefinitionToRelationalModelConverter.Convert(DatabaseDeclaration, "dbo");

                    DataDefinitionExtenderMsSql2016.Extend(DatabaseDeclaration, configuration);
                    RelationalModelExtender.Extend(model, configuration);

                    CreateDatabase(DatabaseDeclaration);

                    Init(configuration, model);
                    Update(configuration, model);
                }
            };
        }

        private void Init(DwhBuilderConfiguration configuration, RelationalModel model)
        {
            var builder = new MsSqlDwhBuilder(PluginTopic, "run#1", EtlRunId1)
            {
                Configuration = configuration,
                ConnectionString = TestConnectionString,
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

            var process = builder.Build();
            Context.ExecuteOne(true, process);

            var result = ReadRows("dbo", "Company");
            Assert.AreEqual(4, result.Count);
            Assert.That.ExactMatch(result, new List<CaseInsensitiveStringKeyDictionary<object>>() {
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 1, ["Name"] = "A", ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(2001, 1, 1, 1, 1, 1, 0), new TimeSpan(0, 0, 0, 0, 0)) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 2, ["Name"] = "B", ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(2001, 1, 1, 1, 1, 1, 0), new TimeSpan(0, 0, 0, 0, 0)) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 3, ["Name"] = "C", ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(2001, 1, 1, 1, 1, 1, 0), new TimeSpan(0, 0, 0, 0, 0)) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 4, ["Name"] = "D", ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(2001, 1, 1, 1, 1, 1, 0), new TimeSpan(0, 0, 0, 0, 0)) } });

            result = ReadRows("dbo", "Company_hist");
            Assert.AreEqual(4, result.Count);
            Assert.That.ExactMatch(result, new List<CaseInsensitiveStringKeyDictionary<object>>() {
                new CaseInsensitiveStringKeyDictionary<object>() { ["Company_histID"] = 1, ["Id"] = 1, ["Name"] = "A", ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(2001, 1, 1, 1, 1, 1, 0), new TimeSpan(0, 0, 0, 0, 0)), ["_ValidTo"] = new DateTimeOffset(new DateTime(2500, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["Company_histID"] = 2, ["Id"] = 2, ["Name"] = "B", ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(2001, 1, 1, 1, 1, 1, 0), new TimeSpan(0, 0, 0, 0, 0)), ["_ValidTo"] = new DateTimeOffset(new DateTime(2500, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["Company_histID"] = 3, ["Id"] = 3, ["Name"] = "C", ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(2001, 1, 1, 1, 1, 1, 0), new TimeSpan(0, 0, 0, 0, 0)), ["_ValidTo"] = new DateTimeOffset(new DateTime(2500, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["Company_histID"] = 4, ["Id"] = 4, ["Name"] = "D", ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(2001, 1, 1, 1, 1, 1, 0), new TimeSpan(0, 0, 0, 0, 0)), ["_ValidTo"] = new DateTimeOffset(new DateTime(2500, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)) } });

            result = ReadRows("dbo", "_temp_Company");
            Assert.AreEqual(4, result.Count);
        }

        private void Update(DwhBuilderConfiguration configuration, RelationalModel model)
        {
            var builder = new MsSqlDwhBuilder(PluginTopic, "run#2", EtlRunId2)
            {
                Configuration = configuration,
                ConnectionString = TestConnectionString,
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

            var process = builder.Build();
            Context.ExecuteOne(true, process);

            var result = ReadRows("dbo", "Company");
            Assert.AreEqual(5, result.Count);
            Assert.That.ExactMatch(result, new List<CaseInsensitiveStringKeyDictionary<object>>() {
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 1, ["Name"] = "A", ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(2001, 1, 1, 1, 1, 1, 0), new TimeSpan(0, 0, 0, 0, 0)) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 2, ["Name"] = "Bx", ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2022, 2, 2, 2, 2, 2, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(2022, 2, 2, 2, 2, 2, 0), new TimeSpan(0, 0, 0, 0, 0)) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 3, ["Name"] = "Cx", ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2022, 2, 2, 2, 2, 2, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(2022, 2, 2, 2, 2, 2, 0), new TimeSpan(0, 0, 0, 0, 0)) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 4, ["Name"] = "D", ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(2001, 1, 1, 1, 1, 1, 0), new TimeSpan(0, 0, 0, 0, 0)) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 5, ["Name"] = "E", ["EtlRunInsert"] = new DateTime(2022, 2, 2, 2, 2, 2, 0), ["EtlRunUpdate"] = new DateTime(2022, 2, 2, 2, 2, 2, 0), ["EtlRunFrom"] = new DateTime(2022, 2, 2, 2, 2, 2, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(2022, 2, 2, 2, 2, 2, 0), new TimeSpan(0, 0, 0, 0, 0)) } });

            result = ReadRows("dbo", "Company_hist");
            Assert.AreEqual(7, result.Count);
            Assert.That.ExactMatch(result, new List<CaseInsensitiveStringKeyDictionary<object>>() {
                new CaseInsensitiveStringKeyDictionary<object>() { ["Company_histID"] = 1, ["Id"] = 1, ["Name"] = "A", ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(2001, 1, 1, 1, 1, 1, 0), new TimeSpan(0, 0, 0, 0, 0)), ["_ValidTo"] = new DateTimeOffset(new DateTime(2500, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["Company_histID"] = 2, ["Id"] = 2, ["Name"] = "B", ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2022, 2, 2, 2, 2, 2, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunTo"] = new DateTime(2022, 2, 2, 2, 2, 2, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(2001, 1, 1, 1, 1, 1, 0), new TimeSpan(0, 0, 0, 0, 0)), ["_ValidTo"] = new DateTimeOffset(new DateTime(2022, 2, 2, 2, 2, 2, 0), new TimeSpan(0, 0, 0, 0, 0)) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["Company_histID"] = 3, ["Id"] = 3, ["Name"] = "C", ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2022, 2, 2, 2, 2, 2, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunTo"] = new DateTime(2022, 2, 2, 2, 2, 2, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(2001, 1, 1, 1, 1, 1, 0), new TimeSpan(0, 0, 0, 0, 0)), ["_ValidTo"] = new DateTimeOffset(new DateTime(2022, 2, 2, 2, 2, 2, 0), new TimeSpan(0, 0, 0, 0, 0)) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["Company_histID"] = 4, ["Id"] = 4, ["Name"] = "D", ["EtlRunInsert"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunUpdate"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["EtlRunFrom"] = new DateTime(2001, 1, 1, 1, 1, 1, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(2001, 1, 1, 1, 1, 1, 0), new TimeSpan(0, 0, 0, 0, 0)), ["_ValidTo"] = new DateTimeOffset(new DateTime(2500, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["Company_histID"] = 5, ["Id"] = 2, ["Name"] = "Bx", ["EtlRunInsert"] = new DateTime(2022, 2, 2, 2, 2, 2, 0), ["EtlRunUpdate"] = new DateTime(2022, 2, 2, 2, 2, 2, 0), ["EtlRunFrom"] = new DateTime(2022, 2, 2, 2, 2, 2, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(2022, 2, 2, 2, 2, 2, 0), new TimeSpan(0, 0, 0, 0, 0)), ["_ValidTo"] = new DateTimeOffset(new DateTime(2500, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["Company_histID"] = 6, ["Id"] = 3, ["Name"] = "Cx", ["EtlRunInsert"] = new DateTime(2022, 2, 2, 2, 2, 2, 0), ["EtlRunUpdate"] = new DateTime(2022, 2, 2, 2, 2, 2, 0), ["EtlRunFrom"] = new DateTime(2022, 2, 2, 2, 2, 2, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(2022, 2, 2, 2, 2, 2, 0), new TimeSpan(0, 0, 0, 0, 0)), ["_ValidTo"] = new DateTimeOffset(new DateTime(2500, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["Company_histID"] = 7, ["Id"] = 5, ["Name"] = "E", ["EtlRunInsert"] = new DateTime(2022, 2, 2, 2, 2, 2, 0), ["EtlRunUpdate"] = new DateTime(2022, 2, 2, 2, 2, 2, 0), ["EtlRunFrom"] = new DateTime(2022, 2, 2, 2, 2, 2, 0), ["_ValidFrom"] = new DateTimeOffset(new DateTime(2022, 2, 2, 2, 2, 2, 0), new TimeSpan(0, 0, 0, 0, 0)), ["_ValidTo"] = new DateTimeOffset(new DateTime(2500, 1, 1, 0, 0, 0, 0), new TimeSpan(0, 0, 0, 0, 0)) } });

            result = ReadRows("dbo", "_temp_Company");
            Assert.AreEqual(3, result.Count);
        }

        public static IEvaluable CreateCompany1(DwhTableBuilder tableBuilder, DateTimeOffset? maxRecordTimestamp)
        {
            return new RowCreator(tableBuilder.ResilientTable.Topic, null)
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

        public static IEvaluable CreateCompany2(DwhTableBuilder tableBuilder, DateTimeOffset? maxRecordTimestamp)
        {
            return new RowCreator(tableBuilder.ResilientTable.Topic, null)
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
}