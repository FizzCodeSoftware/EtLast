namespace FizzCode.EtLast.Tests.Unit.TypeConverters;

[TestClass]
public class BoolConverterTests
{
    [TestMethod]
    public void True()
    {
        var converter = new BoolConverter();
        var result = converter.Convert(true);
        Assert.AreEqual(true, result);
    }

    [TestMethod]
    public void False()
    {
        var converter = new BoolConverter();
        var result = converter.Convert(false);
        Assert.AreEqual(false, result);
    }

    [TestMethod]
    public void TrueByte()
    {
        var converter = new BoolConverter();
        var result = converter.Convert((byte)1);
        Assert.AreEqual(true, result);
    }

    [TestMethod]
    public void TrueSByte()
    {
        var converter = new BoolConverter();
        var result = converter.Convert((sbyte)1);
        Assert.AreEqual(true, result);
    }

    [TestMethod]
    public void TrueShort()
    {
        var converter = new BoolConverter();
        var result = converter.Convert((short)1);
        Assert.AreEqual(true, result);
    }

    [TestMethod]
    public void TrueUShort()
    {
        var converter = new BoolConverter();
        var result = converter.Convert((ushort)1);
        Assert.AreEqual(true, result);
    }

    [TestMethod]
    public void TrueInt()
    {
        var converter = new BoolConverter();
        var result = converter.Convert(1);
        Assert.AreEqual(true, result);
    }

    [TestMethod]
    public void TrueUInt()
    {
        var converter = new BoolConverter();
        var result = converter.Convert(1u);
        Assert.AreEqual(true, result);
    }

    [TestMethod]
    public void TrueLong()
    {
        var converter = new BoolConverter();
        var result = converter.Convert(1L);
        Assert.AreEqual(true, result);
    }

    [TestMethod]
    public void TrueULong()
    {
        var converter = new BoolConverter();
        var result = converter.Convert(1ul);
        Assert.AreEqual(true, result);
    }

    [TestMethod]
    public void TrueString()
    {
        var converter = new BoolConverter();
        var result = converter.Convert("  1");
        Assert.AreEqual(true, result);
    }

    [TestMethod]
    public void FalseString()
    {
        var converter = new BoolConverter();
        var result = converter.Convert("  0  ");
        Assert.AreEqual(false, result);
    }

    [TestMethod]
    public void IndefiniteString()
    {
        var converter = new BoolConverter();
        var result = converter.Convert("  01");
        Assert.AreEqual(null, result);
    }
}
