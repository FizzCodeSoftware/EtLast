namespace FizzCode.EtLast.Tests.Unit.Helpers;

[TestClass]
public class ParthHelpersTests
{
    [TestMethod]
    public void CombineUrl1()
    {
        const string expected = "hello/world";
        var result = PathHelpers.CombineUrl("hello", "/", "world");
        Assert.AreEqual(expected, result);
    }

    [TestMethod]
    public void CombineUrl2()
    {
        const string expected = "hello/world";
        var result = PathHelpers.CombineUrl("hello", "world");
        Assert.AreEqual(expected, result);
    }

    [TestMethod]
    public void CombineUrl3()
    {
        const string expected = "/hello/world";
        var result = PathHelpers.CombineUrl("/", "hello", "world");
        Assert.AreEqual(expected, result);
    }

    [TestMethod]
    public void CombineUrl4()
    {
        const string expected = "/hello/world/";
        var result = PathHelpers.CombineUrl("/", "/", "hello", "/", "world/");
        Assert.AreEqual(expected, result);
    }

    [TestMethod]
    public void CombineUrl5()
    {
        const string expected = "hello";
        var result = PathHelpers.CombineUrl("hello");
        Assert.AreEqual(expected, result);
    }

    [TestMethod]
    public void CombineUrl6()
    {
        const string expected = "/hello/";
        var result = PathHelpers.CombineUrl("/hello/");
        Assert.AreEqual(expected, result);
    }

    [TestMethod]
    public void CombineUrl7()
    {
        const string expected = "/hello/world/";
        var result = PathHelpers.CombineUrl("/", "/hello", "/", "world/");
        Assert.AreEqual(expected, result);
    }

    [TestMethod]
    public void CombineUrl8()
    {
        const string expected = "/hello/nice/world/";
        var result = PathHelpers.CombineUrl("/", "/hello", "/nice", "world/");
        Assert.AreEqual(expected, result);
    }
}
