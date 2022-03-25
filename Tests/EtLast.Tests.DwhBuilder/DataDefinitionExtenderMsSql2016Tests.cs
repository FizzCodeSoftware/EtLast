namespace FizzCode.EtLast.Tests.DwhBuilder;

using System.Linq;
using FizzCode.DbTools.DataDefinition;
using FizzCode.DbTools.DataDefinition.MsSql2016;
using FizzCode.EtLast.DwhBuilder;
using FizzCode.EtLast.DwhBuilder.Extenders.DataDefinition;
using FizzCode.EtLast.DwhBuilder.Extenders.DataDefinition.MsSql;
using Microsoft.VisualStudio.TestTools.UnitTesting;

[TestClass]
public class DataDefinitionExtenderMsSql2016Tests
{
    private class TestModel : DatabaseDeclaration
    {
        public TestModel()
            : base(new MsSql2016TypeMapper(), null, "dbo")
        {
        }

        public SqlTable People { get; } = AddTable(table =>
        {
            table.AddInt("Id").SetPK();
            table.AddNVarChar("Name", 100);
            table.AddInt("FavoritePetId").SetForeignKeyToTable(nameof(SecondaryꜗPet));
        });

        public SqlTable SecondaryꜗPet { get; } = AddTable(table =>
        {
            table.AddInt("Id").SetPK();
            table.AddNVarChar("Name", 100);
            table.AddInt("OwnerPeopleId").SetForeignKeyToTable(nameof(People));
        });
    }

    [TestMethod]
    public void EtlRunInfo()
    {
        var model = new TestModel();
        var configuration = new DwhBuilderConfiguration();

        model.GetTable("Secondary", "Pet").EtlRunInfoDisabled();

        DataDefinitionExtenderMsSql2016.Extend(model, configuration);

        var etlRunTable = model.GetTable("dbo", "_EtlRun");
        Assert.IsNotNull(etlRunTable);
        Assert.AreEqual(6, etlRunTable.Columns.Count);

        Assert.AreEqual(3 + 4, model.GetTable("dbo", "People").Columns.Count);
        Assert.AreEqual(3, model.GetTable("Secondary", "Pet").Columns.Count);
    }

    [TestMethod]
    public void HistoryWithEtlRunInfo()
    {
        var model = new TestModel();
        var configuration = new DwhBuilderConfiguration()
        {
            HistoryTableNamePostfix = "Hist",
        };

        model.GetTable("Secondary", "Pet").EtlRunInfoDisabled();
        model.GetTable("dbo", "People").HasHistoryTable();

        DataDefinitionExtenderMsSql2016.Extend(model, configuration);

        var etlRunTable = model.GetTable("dbo", "_EtlRun");
        Assert.IsNotNull(etlRunTable);

        Assert.AreEqual(3 + 4 + 1, model.GetTable("dbo", "People").Columns.Count);
        Assert.IsTrue(model.GetTable("dbo", "People").Columns.ContainsKey(configuration.EtlRunInsertColumnName));
        Assert.IsTrue(model.GetTable("dbo", "People").Columns.ContainsKey(configuration.EtlRunUpdateColumnName));
        Assert.IsTrue(model.GetTable("dbo", "People").Columns.ContainsKey(configuration.EtlRunFromColumnName));
        Assert.IsTrue(model.GetTable("dbo", "People").Columns.ContainsKey(configuration.EtlRunToColumnName));
        Assert.IsTrue(model.GetTable("dbo", "People").Columns.ContainsKey(configuration.ValidFromColumnName));
        Assert.IsFalse(model.GetTable("dbo", "People").Columns.ContainsKey(configuration.ValidToColumnName));

        Assert.AreEqual(1 + 3 + 4 + 2, model.GetTable("dbo", "PeopleHist").Columns.Count);
        Assert.IsTrue(model.GetTable("dbo", "PeopleHist").Columns.ContainsKey("PeopleHistID"));
        Assert.IsTrue(model.GetTable("dbo", "PeopleHist").Columns.ContainsKey(configuration.EtlRunInsertColumnName));
        Assert.IsTrue(model.GetTable("dbo", "PeopleHist").Columns.ContainsKey(configuration.EtlRunUpdateColumnName));
        Assert.IsTrue(model.GetTable("dbo", "PeopleHist").Columns.ContainsKey(configuration.EtlRunFromColumnName));
        Assert.IsTrue(model.GetTable("dbo", "PeopleHist").Columns.ContainsKey(configuration.EtlRunToColumnName));
        Assert.IsTrue(model.GetTable("dbo", "PeopleHist").Columns.ContainsKey(configuration.ValidFromColumnName));
        Assert.IsTrue(model.GetTable("dbo", "PeopleHist").Columns.ContainsKey(configuration.ValidToColumnName));

        Assert.AreEqual(3, model.GetTable("Secondary", "Pet").Columns.Count);
    }

    [TestMethod]
    public void HistoryWithoutEtlRunInfo()
    {
        var model = new TestModel();
        var configuration = new DwhBuilderConfiguration()
        {
            UseEtlRunInfo = false,
            HistoryTableNamePostfix = "Hist",
        };

        model.GetTable("dbo", "People").HasHistoryTable();

        DataDefinitionExtenderMsSql2016.Extend(model, configuration);

        Assert.AreEqual(3 + 1, model.GetTable("dbo", "People").Columns.Count);
        Assert.IsTrue(model.GetTable("dbo", "People").Columns.ContainsKey(configuration.ValidFromColumnName));
        Assert.IsFalse(model.GetTable("dbo", "People").Columns.ContainsKey(configuration.ValidToColumnName));
        Assert.IsFalse(model.GetTable("dbo", "People").Columns.ContainsKey(configuration.EtlRunInsertColumnName));
        Assert.IsFalse(model.GetTable("dbo", "People").Columns.ContainsKey(configuration.EtlRunUpdateColumnName));

        Assert.AreEqual(1 + 3 + 2, model.GetTable("dbo", "PeopleHist").Columns.Count);
        Assert.IsTrue(model.GetTable("dbo", "PeopleHist").Columns.ContainsKey("PeopleHistID"));
        Assert.IsTrue(model.GetTable("dbo", "PeopleHist").Columns.ContainsKey(configuration.ValidFromColumnName));
        Assert.IsTrue(model.GetTable("dbo", "PeopleHist").Columns.ContainsKey(configuration.ValidToColumnName));
        Assert.IsFalse(model.GetTable("dbo", "People").Columns.ContainsKey(configuration.EtlRunInsertColumnName));
        Assert.IsFalse(model.GetTable("dbo", "People").Columns.ContainsKey(configuration.EtlRunUpdateColumnName));
        Assert.IsFalse(model.GetTable("dbo", "People").Columns.ContainsKey(configuration.EtlRunFromColumnName));
        Assert.IsFalse(model.GetTable("dbo", "People").Columns.ContainsKey(configuration.EtlRunToColumnName));

        Assert.AreEqual(3, model.GetTable("Secondary", "Pet").Columns.Count);

        Assert.IsNotNull(model.GetTable("dbo", "PeopleHist").Properties.OfType<PrimaryKey>().First().SqlColumns[0].SqlColumn.Properties.OfType<Identity>().FirstOrDefault());
        Assert.AreEqual(1, model.GetTable("dbo", "PeopleHist").Properties.OfType<PrimaryKey>().First().SqlColumns.Count);
        Assert.AreEqual("PeopleHistID", model.GetTable("dbo", "PeopleHist").Properties.OfType<PrimaryKey>().First().SqlColumns[0].SqlColumn.Name);

        Assert.AreEqual(model.GetTable("dbo", "People"), model.GetTable("dbo", "PeopleHist").Properties.OfType<ForeignKey>().First().ReferredTable);
        Assert.AreEqual(model.GetTable("dbo", "PeopleHist")["Id"], model.GetTable("dbo", "PeopleHist").Properties.OfType<ForeignKey>().First().ForeignKeyColumns[0].ForeignKeyColumn);
        Assert.AreEqual(model.GetTable("dbo", "People")["Id"], model.GetTable("dbo", "PeopleHist").Properties.OfType<ForeignKey>().First().ForeignKeyColumns[0].ReferredColumn);

        Assert.AreEqual(model.GetTable("Secondary", "Pet"), model.GetTable("dbo", "PeopleHist").Properties.OfType<ForeignKey>().Skip(1).First().ReferredTable);
        Assert.AreEqual(model.GetTable("dbo", "PeopleHist")["FavoritePetId"], model.GetTable("dbo", "PeopleHist").Properties.OfType<ForeignKey>().Skip(1).First().ForeignKeyColumns[0].ForeignKeyColumn);
        Assert.AreEqual(model.GetTable("Secondary", "Pet")["Id"], model.GetTable("dbo", "PeopleHist").Properties.OfType<ForeignKey>().Skip(1).First().ForeignKeyColumns[0].ReferredColumn);
    }
}
