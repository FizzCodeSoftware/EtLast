namespace FizzCode.EtLast.Tests.Integration.Modules.DwhBuilderTests
{
    using System.Collections.Generic;
    using FizzCode.EtLast;
    using FizzCode.EtLast.DwhBuilder;
    using FizzCode.EtLast.DwhBuilder.Extenders.DataDefinition;
    using FizzCode.EtLast.DwhBuilder.Extenders.DataDefinition.MsSql;
    using FizzCode.EtLast.DwhBuilder.MsSql;

    public class HistoryTest : AbstractDwhBuilderTestPlugin
    {
        public override void Execute()
        {
            DatabaseDeclaration.GetTable("dbo", "People").HasHistoryTable();

            var configuration = new DwhBuilderConfiguration();
            var model = DwhDataDefinitionToRelationalModelConverter.Convert(DatabaseDeclaration, "dbo");

            DataDefinitionExtenderMsSql2016.ExtendWithHistoryTables(DatabaseDeclaration, configuration);
            RelationalModelExtender.ExtendWithHistoryTables(model, configuration);

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

            result = ReadRows("sec", "Pet");
        }

        private static IEvaluable CreatePeople(DwhTableBuilder tableBuilder)
        {
            return new RowCreator(tableBuilder.ResilientTable.Topic, null)
            {
                Columns = new[] { "Id", "Name", "FavoritePetId" },
                InputRows = new List<object[]>()
                {
                    new object[] { 0, "A", 2 },
                    new object[] { 1, "B", null },
                    new object[] { 2, "C", 3 },
                    new object[] { 3, "D", null },
                    new object[] { 4, "E", null },
                    new object[] { 5, "F", -1 },
                    new object[] { 5, "F", -1 },
                },
            };
        }

        private static IEvaluable CreatePet(DwhTableBuilder tableBuilder)
        {
            return new RowCreator(tableBuilder.ResilientTable.Topic, null)
            {
                Columns = new[] { "Id", "Name", "OwnerPeopleId" },
                InputRows = new List<object[]>()
                {
                    new object[] { 1, "pet#1", 0 },
                    new object[] { 2, "pet#2", 0 },
                    new object[] { 3, "pet#3", 2 },
                    new object[] { 4, "pet#4", null },
                    new object[] { 5, "pet#5", -1 },
                },
            };
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