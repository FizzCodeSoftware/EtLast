namespace FizzCode.EtLast.Tests.Unit.Rows;

[TestClass]
public class RowKeepNullTests
{
    [TestMethod]
    public void SingleNullColumnResultsNullKey()
    {
        var context = TestExecuter.GetContext();
        context.SetRowType<Row>();

        var initialValues = new Dictionary<string, object>()
        {
            ["name"] = null,
        };

        var row = context.CreateRow(null, initialValues, keepNulls: true);
        var result = row.GenerateKey("name");
        Assert.IsNull(result);
    }

    [TestMethod]
    public void MultipleNullColumnsResultsNonNullKey()
    {
        var context = TestExecuter.GetContext();
        context.SetRowType<Row>();

        var initialValues = new Dictionary<string, object>()
        {
            ["name"] = null,
        };

        var row = context.CreateRow(null, initialValues, keepNulls: true);

        var result = row.GenerateKey("id", "name");
        Assert.IsNotNull(result);
    }

    [TestMethod]
    public void NullValuesAreStored1()
    {
        var context = TestExecuter.GetContext();
        context.SetRowType<Row>();

        var initialValues = new Dictionary<string, object>()
        {
            ["id"] = 12,
            ["name"] = "A",
            ["age"] = null,
        };

        var row = context.CreateRow(null, initialValues, keepNulls: true);
        Assert.AreEqual(3, row.ColumnCount);
        Assert.AreEqual(3, row.Values.Count());
        Assert.IsFalse(row.Values.All(kvp => kvp.Value != null));

        row["age"] = 7;
        Assert.AreEqual(3, row.ColumnCount);
        Assert.AreEqual(3, row.Values.Count());
        Assert.IsTrue(row.Values.All(kvp => kvp.Value != null));

        row["name"] = null;
        Assert.AreEqual(3, row.ColumnCount);
        Assert.AreEqual(3, row.Values.Count());
        Assert.IsFalse(row.Values.All(kvp => kvp.Value != null));
    }

    [TestMethod]
    public void NullValuesAreNotStored2()
    {
        var context = TestExecuter.GetContext();
        context.SetRowType<Row>();

        var initialValues = new Dictionary<string, object>()
        {
            ["id"] = 12,
            ["name"] = "A",
        };

        var row = context.CreateRow(null, initialValues, keepNulls: true);
        Assert.AreEqual(2, row.ColumnCount);
        Assert.AreEqual(2, row.Values.Count());

        row["id"] = null;
        Assert.AreEqual(2, row.ColumnCount);
        Assert.AreEqual(2, row.Values.Count());

        row["trash"] = null;
        Assert.AreEqual(3, row.ColumnCount);
        Assert.AreEqual(3, row.Values.Count());
    }
}
