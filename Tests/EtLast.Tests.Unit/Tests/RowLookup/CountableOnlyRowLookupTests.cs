namespace FizzCode.EtLast.Tests.Unit.Rows;

[TestClass]
public class CountableOnlyRowLookupTests
{
    [TestMethod]
    public void NotNullIdentity()
    {
        var context = TestExecuter.GetContext();
        var lookup = new CountOnlyRowLookup();
        var builder = new RowLookupBuilder()
        {
            Process = TestData.Person(),
            KeyGenerator = row => row.GenerateKey("id"),
        };

        builder.AddTo(lookup, context);
        Assert.AreEqual(7, lookup.Keys.Count());
        Assert.AreEqual(7, lookup.Count);
        Assert.AreEqual(1, lookup.CountByKey("0"));
        Assert.AreEqual(1, lookup.CountByKey("1"));
        Assert.AreEqual(1, lookup.CountByKey("2"));
        Assert.AreEqual(1, lookup.CountByKey("3"));
        Assert.AreEqual(1, lookup.CountByKey("4"));
        Assert.AreEqual(1, lookup.CountByKey("5"));
        Assert.AreEqual(1, lookup.CountByKey("6"));
    }

    [TestMethod]
    public void NullableIdentity()
    {
        var context = TestExecuter.GetContext();
        var lookup = new CountOnlyRowLookup();
        var builder = new RowLookupBuilder()
        {
            Process = TestData.Person(),
            KeyGenerator = row => row.GenerateKey("age"),
        };

        builder.AddTo(lookup, context);
        Assert.AreEqual(6, lookup.Keys.Count());
        Assert.AreEqual(6, lookup.Count);
        Assert.AreEqual(1, lookup.CountByKey("17"));
        Assert.AreEqual(1, lookup.CountByKey("8"));
        Assert.AreEqual(1, lookup.CountByKey("27"));
        Assert.AreEqual(1, lookup.CountByKey("39"));
        Assert.AreEqual(1, lookup.CountByKey("-3"));
        Assert.AreEqual(1, lookup.CountByKey("11"));
        Assert.AreEqual(0, lookup.CountByKey(null));
    }

    [TestMethod]
    public void NullableMulti()
    {
        var context = TestExecuter.GetContext();
        var lookup = new CountOnlyRowLookup();
        var builder = new RowLookupBuilder()
        {
            Process = TestData.Person(),
            KeyGenerator = row => row.GenerateKey("name"),
        };

        builder.AddTo(lookup, context);
        Assert.AreEqual(6, lookup.Keys.Count());
        Assert.AreEqual(2, lookup.CountByKey("A"));
        Assert.AreEqual(1, lookup.CountByKey("B"));
        Assert.AreEqual(1, lookup.CountByKey("C"));
        Assert.AreEqual(1, lookup.CountByKey("D"));
        Assert.AreEqual(1, lookup.CountByKey("E"));
        Assert.AreEqual(1, lookup.CountByKey("fake"));
    }
}
