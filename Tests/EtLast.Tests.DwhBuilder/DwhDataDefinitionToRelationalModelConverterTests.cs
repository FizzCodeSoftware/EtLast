namespace FizzCode.EtLast.Tests.DwhBuilder
{
    using FizzCode.DbTools.DataDefinition;
    using FizzCode.DbTools.DataDefinition.MsSql2016;
    using FizzCode.EtLast.DwhBuilder;
    using FizzCode.EtLast.DwhBuilder.Extenders.DataDefinition;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class DwhDataDefinitionToRelationalModelConverterTests
    {
        private class TestModel : DatabaseDeclaration
        {
            public TestModel()
                : base(new MsSql2016TypeMapper(), null, "dbo")
            {
            }

            public SqlTable People { get; } = AddTable(table =>
            {
                table.HasHistoryTable();
                table.AddInt("Id").SetPK().RecordTimestampIndicator();
                table.AddNVarChar("Name", 100).HistoryDisabled();
                table.AddInt("FavoritePetId").SetForeignKeyToTable(nameof(SecondaryꜗPet));
            });

            public SqlTable SecondaryꜗPet { get; } = AddTable(table =>
            {
                table.SourceTableNameOverride("animal");
                table.AddInt("Id").SetPK();
                table.AddNVarChar("Name", 100);
                table.AddInt("OwnerPeopleId").SetForeignKeyToTable(nameof(People));
            });
        }

        [TestMethod]
        public void EtlRunInfo()
        {
            var sourceModel = new TestModel();
            sourceModel.GetTable("Secondary", "Pet").EtlRunInfoDisabled();

            var targetModel = DwhDataDefinitionToRelationalModelConverter.Convert(sourceModel, "dbo");

            Assert.AreEqual(2, targetModel.Schemas.Count);
            Assert.AreEqual(1, targetModel["dbo"].Tables.Count);
            Assert.AreEqual(1, targetModel["secondary"].Tables.Count);
            Assert.AreEqual(3, targetModel["dbo"]["people"].Columns.Count);
            Assert.AreEqual(3, targetModel["secondary"]["PeT"].Columns.Count);
            Assert.AreEqual(1, targetModel["dbo"]["PEOPLE"].PrimaryKeyColumns.Count);
            Assert.AreEqual(1, targetModel["secondarY"]["pet"].PrimaryKeyColumns.Count);
            Assert.AreEqual(1, targetModel["dbo"]["PEOPLE"].ForeignKeys.Count);
            Assert.AreEqual(1, targetModel["SECONDARY"]["pet"].ForeignKeys.Count);

            Assert.IsFalse(targetModel["dbo"]["people"].GetEtlRunInfoDisabled());
            Assert.IsTrue(targetModel["secondary"]["PeT"].GetEtlRunInfoDisabled());

            Assert.IsTrue(targetModel["dbo"]["people"].GetHasHistoryTable());
            Assert.IsFalse(targetModel["secondary"]["PeT"].GetHasHistoryTable());

            Assert.IsFalse(targetModel["dbo"]["people"]["id"].GetHistoryDisabled());
            Assert.IsTrue(targetModel["dbo"]["people"]["name"].GetHistoryDisabled());

            Assert.IsTrue(targetModel["dbo"]["people"]["id"].GetRecordTimestampIndicator());
            Assert.IsFalse(targetModel["dbo"]["people"]["name"].GetRecordTimestampIndicator());

            Assert.IsNull(targetModel["dbo"]["people"].GetSourceTableNameOverride());
            Assert.AreEqual("animal", targetModel["secondary"]["pet"].GetSourceTableNameOverride());
        }
    }
}