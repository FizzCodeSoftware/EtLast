namespace FizzCode.EtLast.Tests.Unit.TypeConverters;

[TestClass]
public class BoolConverterAutoTests
{
    [TestMethod]
    public void YesVaryingCase()
    {
        var converter = new BoolConverterAuto();
        var result = converter.Convert("yeS");
        Assert.AreEqual(true, result);
    }

    [TestMethod]
    public void NoVaryingCase()
    {
        var converter = new BoolConverterAuto();
        var result = converter.Convert("nO");
        Assert.AreEqual(false, result);
    }

    [TestMethod]
    public void QuestionMark()
    {
        var converter = new BoolConverterAuto();
        var result = converter.Convert("?");
        Assert.AreEqual(null, result);
    }

    [TestMethod]
    public void TrueVaryingCase()
    {
        var converter = new BoolConverterAuto();
        var result = converter.Convert("tRUe");
        Assert.AreEqual(true, result);
    }

    [TestMethod]
    public void FalseVaryingCase()
    {
        var converter = new BoolConverterAuto();
        var result = converter.Convert("faLse");
        Assert.AreEqual(false, result);
    }

    [TestMethod]
    public void KnownTrueString()
    {
        var converter = new BoolConverterAuto()
        {
            KnownTrueString = "ofcourse",
        };

        var result = converter.Convert(" ofcourse");
        Assert.AreEqual(true, result);
    }

    [TestMethod]
    public void KnownFalseString()
    {
        var converter = new BoolConverterAuto()
        {
            KnownFalseString = "ofcourseNOT",
        };

        var result = converter.Convert(" OFcourseNOT  ");
        Assert.AreEqual(false, result);
    }
}
