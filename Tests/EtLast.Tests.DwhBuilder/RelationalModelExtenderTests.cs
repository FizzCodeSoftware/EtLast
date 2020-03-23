namespace FizzCode.EtLast.Tests.DwhBuilder
{
    using FizzCode.EtLast.DwhBuilder;
    using FizzCode.LightWeight.RelationalModel;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class RelationalModelExtenderTests
    {
        private RelationalModel CreateModel()
        {
            var model = new RelationalModel("dbo");
            var dbo = model["dbo"];
            var secondary = model.AddSchema("secondary");

            var people = dbo.AddTable("people");
            var peopleId = people.AddColumn("id", true);
            people.AddColumn("name", false);
            var peopleFavoritePetId = people.AddColumn("favoritePetId", false);

            var pet = secondary.AddTable("pet");
            var petId = pet.AddColumn("id", true);
            pet.AddColumn("name", false);
            var petOwnerId = pet.AddColumn("ownerPeopleId", false);

            people.AddForeignKeyTo(pet)
                .AddColumnPair(peopleFavoritePetId, petId);

            pet.AddForeignKeyTo(people)
                .AddColumnPair(petOwnerId, peopleId);

            return model;
        }

        [TestMethod]
        public void EtlRunInfo()
        {
            var model = CreateModel();
            var configuration = new DwhBuilderConfiguration();

            model["secondary"]["pet"].SetEtlRunInfoDisabled();

            RelationalModelExtender.ExtendWithEtlRunInfo(model.DefaultSchema, configuration);

            var etlRunTable = model["dbo"]["_EtlRun"];
            Assert.IsNotNull(etlRunTable);
            Assert.AreEqual(7, etlRunTable.Columns.Count);

            Assert.AreEqual(3 + 2, model["dbo"]["people"].Columns.Count);
            Assert.AreEqual(3, model["secondary"]["pet"].Columns.Count);
        }

        [TestMethod]
        public void History()
        {
            var model = CreateModel();
            var configuration = new DwhBuilderConfiguration()
            {
                HistoryTableNamePostfix = ".hist",
            };

            model["dbo"]["people"].SetHasHistoryTable();

            RelationalModelExtender.ExtendWithHistoryTables(model, configuration);

            Assert.AreEqual(3 + 1, model["dbo"]["people"].Columns.Count);
            Assert.AreEqual(configuration.ValidFromColumnName, model["dbo"]["people"].Columns[3].Name);

            Assert.AreEqual(1 + 3 + 2, model["dbo"]["people.hist"].Columns.Count);
            Assert.AreEqual("people.histID", model["dbo"]["people.hist"].Columns[0].Name);
            Assert.AreEqual(configuration.ValidFromColumnName, model["dbo"]["people.hist"].Columns[4].Name);
            Assert.AreEqual(configuration.ValidToColumnName, model["dbo"]["people.hist"].Columns[5].Name);
            Assert.AreEqual(3, model["secondary"]["pet"].Columns.Count);

            Assert.IsTrue(model["dbo"]["people.hist"].AnyPrimaryKeyColumnIsIdentity);
            Assert.AreEqual(1, model["dbo"]["people.hist"].PrimaryKeyColumns.Count);
            Assert.AreEqual("people.histID", model["dbo"]["people.hist"].PrimaryKeyColumns[0].Name);

            Assert.AreEqual(model["dbo"]["people"], model["dbo"]["people.hist"].ForeignKeys[0].TargetTable);
            Assert.AreEqual(model["dbo"]["people.hist"]["id"], model["dbo"]["people.hist"].ForeignKeys[0].ColumnPairs[0].SourceColumn);
            Assert.AreEqual(model["dbo"]["people"]["id"], model["dbo"]["people.hist"].ForeignKeys[0].ColumnPairs[0].TargetColumn);

            Assert.AreEqual(model["secondary"]["PET"], model["dbo"]["people.hist"].ForeignKeys[1].TargetTable);
            Assert.AreEqual(model["dbo"]["people.hist"]["favoritepetid"], model["dbo"]["people.hist"].ForeignKeys[1].ColumnPairs[0].SourceColumn);
            Assert.AreEqual(model["secondary"]["PET"]["id"], model["dbo"]["people.hist"].ForeignKeys[1].ColumnPairs[0].TargetColumn);
        }
    }
}