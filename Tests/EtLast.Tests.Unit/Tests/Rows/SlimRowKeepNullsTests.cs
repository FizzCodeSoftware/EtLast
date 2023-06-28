namespace FizzCode.EtLast.Tests.Unit.Rows;

[TestClass]
public class SlimRowKeepNullsTests
{
    [TestMethod]
    public void SingleNullColumnResultsNullKey()
    {
        var values = new SlimRow
        {
            KeepNulls = true,
            ["name"] = null,
        };

        var result = values.GenerateKey("name");
        Assert.IsNull(result);
    }

    [TestMethod]
    public void MultipleNullColumnsResultsNonNullKey()
    {
        var values = new SlimRow
        {
            KeepNulls = true,
            ["name"] = null,
        };

        var result = values.GenerateKey("id", "name");
        Assert.IsNotNull(result);
    }

    [TestMethod]
    public void NullValuesAreStored()
    {
        var values = new SlimRow()
        {
            KeepNulls = true,
            ["id"] = 12,
            ["name"] = "A",
            ["age"] = null,
        };

        Assert.AreEqual(3, values.ColumnCount);
        Assert.AreEqual(3, values.Values.Count());
        Assert.IsFalse(values.Values.All(kvp => kvp.Value != null));

        values["age"] = 7;
        Assert.AreEqual(3, values.ColumnCount);
        Assert.IsTrue(values.Values.All(kvp => kvp.Value != null));

        values["name"] = null;
        Assert.AreEqual(3, values.ColumnCount);
        Assert.IsFalse(values.Values.All(kvp => kvp.Value != null));
    }
}
