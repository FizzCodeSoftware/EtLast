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

            RelationalModelExtender.Extend(model, configuration);

            var etlRunTable = model["dbo"]["_EtlRun"];
            Assert.IsNotNull(etlRunTable);
            Assert.AreEqual(6, etlRunTable.Columns.Count);

            Assert.AreEqual(3 + 4, model["dbo"]["people"].Columns.Count);
            Assert.AreEqual(3, model["secondary"]["pet"].Columns.Count);
        }

        [TestMethod]
        public void HistoryWithEtlRunInfo()
        {
            var model = CreateModel();
            var configuration = new DwhBuilderConfiguration()
            {
                HistoryTableNamePostfix = "Hist",
            };

            model["secondary"]["pet"].SetEtlRunInfoDisabled();
            model["dbo"]["people"].SetHasHistoryTable();

            RelationalModelExtender.Extend(model, configuration);

            Assert.AreEqual(3 + 4 + 1, model["dbo"]["people"].Columns.Count);
            Assert.IsNotNull(model["dbo"]["people"][configuration.EtlRunInsertColumnName]);
            Assert.IsNotNull(model["dbo"]["people"][configuration.EtlRunUpdateColumnName]);
            Assert.IsNotNull(model["dbo"]["people"][configuration.EtlRunFromColumnName]);
            Assert.IsNotNull(model["dbo"]["people"][configuration.EtlRunToColumnName]);
            Assert.IsNotNull(model["dbo"]["people"][configuration.ValidFromColumnName]);

            Assert.AreEqual(1 + 3 + 4 + 2, model["dbo"]["peopleHist"].Columns.Count);
            Assert.AreEqual("peopleHistID", model["dbo"]["peopleHist"].Columns[0].Name);
            Assert.IsNotNull(model["dbo"]["peopleHist"][configuration.EtlRunInsertColumnName]);
            Assert.IsNotNull(model["dbo"]["peopleHist"][configuration.EtlRunUpdateColumnName]);
            Assert.IsNotNull(model["dbo"]["peopleHist"][configuration.EtlRunFromColumnName]);
            Assert.IsNotNull(model["dbo"]["peopleHist"][configuration.EtlRunToColumnName]);
            Assert.IsNotNull(model["dbo"]["peopleHist"][configuration.ValidFromColumnName]);
            Assert.IsNotNull(model["dbo"]["peopleHist"][configuration.ValidToColumnName]);

            Assert.AreEqual(3, model["secondary"]["pet"].Columns.Count);
        }

        [TestMethod]
        public void HistoryWithoutEtlRunInfo()
        {
            var model = CreateModel();
            var configuration = new DwhBuilderConfiguration()
            {
                UseEtlRunInfo = false,
                HistoryTableNamePostfix = "Hist",
            };

            model["dbo"]["people"].SetHasHistoryTable();

            RelationalModelExtender.Extend(model, configuration);

            Assert.AreEqual(3 + 1, model["dbo"]["people"].Columns.Count);
            Assert.AreEqual(configuration.ValidFromColumnName, model["dbo"]["people"].Columns[3].Name);

            Assert.AreEqual(1 + 3 + 2, model["dbo"]["peopleHist"].Columns.Count);
            Assert.AreEqual("peopleHistID", model["dbo"]["peopleHist"].Columns[0].Name);
            Assert.AreEqual(configuration.ValidFromColumnName, model["dbo"]["peopleHist"].Columns[4].Name);
            Assert.AreEqual(configuration.ValidToColumnName, model["dbo"]["peopleHist"].Columns[5].Name);

            Assert.AreEqual(3, model["secondary"]["pet"].Columns.Count);

            Assert.IsTrue(model["dbo"]["peopleHist"].AnyPrimaryKeyColumnIsIdentity);
            Assert.AreEqual(1, model["dbo"]["peopleHist"].PrimaryKeyColumns.Count);
            Assert.AreEqual("peopleHistID", model["dbo"]["peopleHist"].PrimaryKeyColumns[0].Name);

            Assert.AreEqual(1 + 3 + 2, model["dbo"]["peopleHist"].Columns.Count);
            Assert.IsTrue(model["dbo"]["peopleHist"]["PeopleHistID"] != null);
            Assert.IsTrue(model["dbo"]["peopleHist"][configuration.ValidFromColumnName] != null);
            Assert.IsTrue(model["dbo"]["peopleHist"][configuration.ValidToColumnName] != null);

            Assert.AreEqual(model["dbo"]["people"], model["dbo"]["peopleHist"].ForeignKeys[0].TargetTable);
            Assert.AreEqual(model["dbo"]["peopleHist"]["id"], model["dbo"]["peopleHist"].ForeignKeys[0].ColumnPairs[0].SourceColumn);
            Assert.AreEqual(model["dbo"]["people"]["id"], model["dbo"]["peopleHist"].ForeignKeys[0].ColumnPairs[0].TargetColumn);

            Assert.AreEqual(model["secondary"]["PET"], model["dbo"]["peopleHist"].ForeignKeys[1].TargetTable);
            Assert.AreEqual(model["dbo"]["peopleHist"]["favoritepetid"], model["dbo"]["peopleHist"].ForeignKeys[1].ColumnPairs[0].SourceColumn);
            Assert.AreEqual(model["secondary"]["PET"]["id"], model["dbo"]["peopleHist"].ForeignKeys[1].ColumnPairs[0].TargetColumn);
        }
    }
}