namespace FizzCode.EtLast.Tests.Integration.Modules.DwhBuilderTests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using FizzCode.EtLast;
    using FizzCode.EtLast.AdoNet;
    using FizzCode.EtLast.DwhBuilder;
    using FizzCode.EtLast.DwhBuilder.Extenders.DataDefinition;
    using FizzCode.EtLast.DwhBuilder.Extenders.DataDefinition.MsSql;
    using FizzCode.EtLast.DwhBuilder.MsSql;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    public class EtlRunInfoTest : AbstractDwhBuilderTestPlugin
    {
        public override void Execute()
        {
            DatabaseDeclaration.GetTable("sec", "Pet").EtlRunInfoDisabled();

            var configuration = new DwhBuilderConfiguration();
            var model = DwhDataDefinitionToRelationalModelConverter.Convert(DatabaseDeclaration, "dbo");

            DataDefinitionExtenderMsSql2016.ExtendWithEtlRunInfo(DatabaseDeclaration, configuration);
            RelationalModelExtender.ExtendWithEtlRunInfo(model["dbo"], configuration);

            CreateDatabase(DatabaseDeclaration);

            var builder = new DwhBuilder(PluginTopic, "test")
            {
                Configuration = configuration,
                ConnectionString = TestConnectionString,
                Model = model,
            };

            builder.AddTables(model["dbo"]["People"])
                .InputIsCustomProcess(CreatePeople)
                .AddMutators(PeopleMutators)
                .DisableConstraintCheck()
                .BaseIsCurrentFinalizer(b => b
                    .MatchByPrimaryKey());

            builder.AddTables(model["sec"]["Pet"])
                .InputIsCustomProcess(CreatePet)
                .AddMutators(PetMutators)
                .DisableConstraintCheck()
                .BaseIsCurrentFinalizer(b => b
                    .MatchByPrimaryKey());

            var process = builder.Build();
            Context.ExecuteOne(true, process);

            var result = ReadRows("dbo", "People");
            Assert.AreEqual(5, result.Count);
            Assert.That.ExactMatch(result, new List<Dictionary<string, object>>() {
                new Dictionary<string, object>() { ["Id"] = 0, ["Name"] = "A", ["FavoritePetId"] = 2, ["EtlRunInsert"] = new DateTimeOffset(new DateTime(2020, 2, 2, 12, 0, 0, 0), new TimeSpan(0, 2, 0, 0,0)), ["EtlRunUpdate"] = new DateTimeOffset(new DateTime(2020, 2, 2, 12, 0, 0, 0), new TimeSpan(0, 2, 0, 0,0)) },
                new Dictionary<string, object>() { ["Id"] = 1, ["Name"] = "B", ["EtlRunInsert"] = new DateTimeOffset(new DateTime(2020, 2, 2, 12, 0, 0, 0), new TimeSpan(0, 2, 0, 0,0)), ["EtlRunUpdate"] = new DateTimeOffset(new DateTime(2020, 2, 2, 12, 0, 0, 0), new TimeSpan(0, 2, 0, 0,0)) },
                new Dictionary<string, object>() { ["Id"] = 2, ["Name"] = "C", ["FavoritePetId"] = 3, ["EtlRunInsert"] = new DateTimeOffset(new DateTime(2020, 2, 2, 12, 0, 0, 0), new TimeSpan(0, 2, 0, 0,0)), ["EtlRunUpdate"] = new DateTimeOffset(new DateTime(2020, 2, 2, 12, 0, 0, 0), new TimeSpan(0, 2, 0, 0,0)) },
                new Dictionary<string, object>() { ["Id"] = 3, ["Name"] = "D", ["EtlRunInsert"] = new DateTimeOffset(new DateTime(2020, 2, 2, 12, 0, 0, 0), new TimeSpan(0, 2, 0, 0,0)), ["EtlRunUpdate"] = new DateTimeOffset(new DateTime(2020, 2, 2, 12, 0, 0, 0), new TimeSpan(0, 2, 0, 0,0)) },
                new Dictionary<string, object>() { ["Id"] = 4, ["Name"] = "E", ["EtlRunInsert"] = new DateTimeOffset(new DateTime(2020, 2, 2, 12, 0, 0, 0), new TimeSpan(0, 2, 0, 0,0)), ["EtlRunUpdate"] = new DateTimeOffset(new DateTime(2020, 2, 2, 12, 0, 0, 0), new TimeSpan(0, 2, 0, 0,0)) } });

            result = ReadRows("sec", "Pet");
            Assert.AreEqual(3, result.Count);
            Assert.That.ExactMatch(result, new List<Dictionary<string, object>>() {
                new Dictionary<string, object>() { ["Id"] = 1, ["Name"] = "pet#1", ["OwnerPeopleId"] = 0 },
                new Dictionary<string, object>() { ["Id"] = 2, ["Name"] = "pet#2", ["OwnerPeopleId"] = 0 },
                new Dictionary<string, object>() { ["Id"] = 3, ["Name"] = "pet#3", ["OwnerPeopleId"] = 2 } });
        }

        protected List<ISlimRow> ReadRows(string schema, string table)
        {
            return new AdoNetDbReader(PluginTopic, null)
            {
                ConnectionString = TestConnectionString,
                TableName = TestConnectionString.Escape(table, schema),
            }.Evaluate().TakeRowsAndReleaseOwnership().ToList();
        }

        private IEnumerable<IMutator> PeopleMutators(DwhTableBuilder tableBuilder)
        {
            yield return new CustomMutator(tableBuilder.ResilientTable.Topic, "FkFix")
            {
                Then = (proc, row) =>
                {
                    var fk = row.GetAs<int?>("FavoritePetId");
                    return fk == null || fk.Value >= 0;
                },
            };
        }

        private IEnumerable<IMutator> PetMutators(DwhTableBuilder tableBuilder)
        {
            yield return new CustomMutator(tableBuilder.ResilientTable.Topic, "FkFix")
            {
                Then = (proc, row) =>
                {
                    var fk = row.GetAs<int?>("OwnerPeopleId");
                    return fk != null && fk.Value >= 0;
                },
            };
        }
    }
}