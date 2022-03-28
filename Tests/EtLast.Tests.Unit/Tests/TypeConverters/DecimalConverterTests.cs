namespace FizzCode.EtLast.Tests.Unit.TypeConverters;

[TestClass]
public class DecimalConverterTests
{
    [TestMethod]
    public void InvString()
    {
        var converter = new DecimalConverter();
        var result = converter.Convert("1.234");
        Assert.AreEqual(1.234m, result);
    }

    [TestMethod]
    public void InvStringThousands()
    {
        var converter = new DecimalConverter();
        var result = converter.Convert("1234.456");
        Assert.AreEqual(1234.456m, result);
    }

    [TestMethod]
    public void InvStringThousandsTooBig()
    {
        var converter = new DecimalConverter();
        var result = converter.Convert("123456789012345678901234567890.456");
        Assert.AreEqual(null, result);
    }

    [TestMethod]
    public void FromSByte()
    {
        var converter = new DecimalConverter();
        var result = converter.Convert((sbyte)77);
        Assert.AreEqual(77m, result);
    }

    [TestMethod]
    public void FromByte()
    {
        var converter = new DecimalConverter();
        var result = converter.Convert((byte)77);
        Assert.AreEqual(77m, result);
    }

    [TestMethod]
    public void FromShort()
    {
        var converter = new DecimalConverter();
        var result = converter.Convert((short)77);
        Assert.AreEqual(77m, result);
    }

    [TestMethod]
    public void FromUShort()
    {
        var converter = new DecimalConverter();
        var result = converter.Convert((ushort)77);
        Assert.AreEqual(77m, result);
    }

    [TestMethod]
    public void FromInt()
    {
        var converter = new DecimalConverter();
        var result = converter.Convert(77);
        Assert.AreEqual(77m, result);
    }

    [TestMethod]
    public void FromUInt()
    {
        var converter = new DecimalConverter();
        var result = converter.Convert(77u);
        Assert.AreEqual(77m, result);
    }

    [TestMethod]
    public void FromLong()
    {
        var converter = new DecimalConverter();
        var result = converter.Convert(long.MaxValue);
        Assert.AreEqual((decimal)long.MaxValue, result);
    }

    [TestMethod]
    public void FromULong()
    {
        var converter = new DecimalConverter();
        var result = converter.Convert(ulong.MaxValue);
        Assert.AreEqual((decimal)ulong.MaxValue, result);
    }

    [TestMethod]
    public void FromFloat()
    {
        var converter = new DecimalConverter();
        var result = converter.Convert(4f / 5f);
        Assert.AreEqual((decimal)(4f / 5f), result);
    }

    [TestMethod]
    public void FromDouble()
    {
        var converter = new DecimalConverter();
        var result = converter.Convert(4d / 5d);
        Assert.AreEqual((decimal)(4d / 5d), result);
    }

    [TestMethod]
    public void FromDecimal()
    {
        var converter = new DecimalConverter();
        var result = converter.Convert(71m / 49m);
        Assert.AreEqual(71m / 49m, result);
    }
}
