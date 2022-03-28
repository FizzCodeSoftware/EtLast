namespace FizzCode.EtLast.Tests.Unit.TypeConverters;

[TestClass]
public class DecimalConverterAutoTests
{
    [TestMethod]
    public void InvInv()
    {
        var converter = new DecimalConverterAuto(CultureInfo.InvariantCulture);
        var result = converter.Convert("1.234");
        Assert.AreEqual(1.234m, result);
    }

    [TestMethod]
    public void InvInvNeg()
    {
        var converter = new DecimalConverterAuto(CultureInfo.InvariantCulture);
        var result = converter.Convert("-1.234");
        Assert.AreEqual(-1.234m, result);
    }

    [TestMethod]
    public void HuHu()
    {
        var converter = new DecimalConverterAuto(new CultureInfo("hu-HU"));
        var result = converter.Convert("1,234");
        Assert.AreEqual(1.234m, result);
    }

    [TestMethod]
    public void HuHuPos()
    {
        var converter = new DecimalConverterAuto(new CultureInfo("hu-HU"));
        var result = converter.Convert("+1,234");
        Assert.AreEqual(1.234m, result);
    }

    [TestMethod]
    public void HuHuPosWhiteSpace()
    {
        var converter = new DecimalConverterAuto(new CultureInfo("hu-HU"));
        var result = converter.Convert(" +1,234  ");
        Assert.AreEqual(1.234m, result);
    }

    [TestMethod]
    public void HuHuPosBrokenWhiteSpace()
    {
        var converter = new DecimalConverterAuto(new CultureInfo("hu-HU"));
        var result = converter.Convert("+ 1,234");
        Assert.AreEqual(null, result);
    }

    [TestMethod]
    public void HuHuThousandsWhiteSpace()
    {
        var converter = new DecimalConverterAuto(new CultureInfo("hu-HU"));
        var result = converter.Convert("1   234 456,2 ");
        Assert.AreEqual(1234456.2m, result);
    }

    [TestMethod]
    public void HuHuThousandsNegWhiteSpace()
    {
        var converter = new DecimalConverterAuto(new CultureInfo("hu-HU"));
        var result = converter.Convert("-1 234 456,2");
        Assert.AreEqual(-1234456.2m, result);
    }

    [TestMethod]
    public void HuHuThousandsNegBrokenWhiteSpace()
    {
        var converter = new DecimalConverterAuto(new CultureInfo("hu-HU"));
        var result = converter.Convert("- 1 234 456,2");
        Assert.AreEqual(null, result);
    }

    [TestMethod]
    public void HuInvFallback()
    {
        var converter = new DecimalConverterAuto(new CultureInfo("hu-HU"));
        var result = converter.Convert("1.234");
        Assert.AreEqual(1.234m, result);
    }

    [TestMethod]
    public void HuEnStringWithThousandsFallbackToInvariant()
    {
        var converter = new DecimalConverterAuto(new CultureInfo("hu-HU"));
        var result = converter.Convert("1,234,456.2");
        Assert.AreEqual(1234456.2m, result);
    }

    [TestMethod]
    public void InvHuStringMisrecognizedThousandSeparator()
    {
        var converter = new DecimalConverterAuto(CultureInfo.InvariantCulture);
        var result = converter.Convert("123,2");
        Assert.AreEqual(1232.0m, result);
    }

    [TestMethod]
    public void InvHuStringRecognizedThousandSeparator()
    {
        var converter = new DecimalConverterAuto(new CultureInfo("hu-HU"));
        var result = converter.Convert("123,2");
        Assert.AreEqual(123.2m, result);
    }

    [TestMethod]
    public void InvHuStringWithThousandsBroken()
    {
        var converter = new DecimalConverterAuto(CultureInfo.InvariantCulture);
        var result = converter.Convert("1 234 456,2");
        Assert.AreEqual(null, result);
    }

    [TestMethod]
    public void UsUs()
    {
        var converter = new DecimalConverterAuto(new CultureInfo("en-US"));
        var result = converter.Convert("123.2");
        Assert.AreEqual(123.2m, result);
    }

    [TestMethod]
    public void UsUsThousands()
    {
        var converter = new DecimalConverterAuto(new CultureInfo("en-US"));
        var result = converter.Convert("1,234,456.2");
        Assert.AreEqual(1234456.2m, result);
    }

    [TestMethod]
    public void UsInvFallback()
    {
        var converter = new DecimalConverterAuto(new CultureInfo("en-US"));
        var result = converter.Convert("1234456.2");
        Assert.AreEqual(1234456.2m, result);
    }

    [TestMethod]
    public void UsInvBroken()
    {
        var converter = new DecimalConverterAuto(new CultureInfo("en-US"));
        var result = converter.Convert("1.234.456,2");
        Assert.AreEqual(null, result);
    }
}
