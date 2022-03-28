namespace FizzCode.EtLast.Tests.Unit.Helpers;

[TestClass]
public class ParthHelpersTests
{
    [TestMethod]
    public void CombineUrl1()
    {
        var expected = "hello/world";
        var result = PathHelpers.CombineUrl("hello", "/", "world");
        Assert.AreEqual(expected, result);
    }

    [TestMethod]
    public void CombineUrl2()
    {
        var expected = "hello/world";
        var result = PathHelpers.CombineUrl("hello", "world");
        Assert.AreEqual(expected, result);
    }

    [TestMethod]
    public void CombineUrl3()
    {
        var expected = "/hello/world";
        var result = PathHelpers.CombineUrl("/", "hello", "world");
        Assert.AreEqual(expected, result);
    }

    [TestMethod]
    public void CombineUrl4()
    {
        var expected = "/hello/world/";
        var result = PathHelpers.CombineUrl("/", "/", "hello", "/", "world/");
        Assert.AreEqual(expected, result);
    }

    [TestMethod]
    public void CombineUrl5()
    {
        var expected = "hello";
        var result = PathHelpers.CombineUrl("hello");
        Assert.AreEqual(expected, result);
    }

    [TestMethod]
    public void CombineUrl6()
    {
        var expected = "/hello/";
        var result = PathHelpers.CombineUrl("/hello/");
        Assert.AreEqual(expected, result);
    }

    [TestMethod]
    public void CombineUrl7()
    {
        var expected = "/hello/world/";
        var result = PathHelpers.CombineUrl("/", "/hello", "/", "world/");
        Assert.AreEqual(expected, result);
    }

    [TestMethod]
    public void CombineUrl8()
    {
        var expected = "/hello/nice/world/";
        var result = PathHelpers.CombineUrl("/", "/hello", "/nice", "world/");
        Assert.AreEqual(expected, result);
    }
}
