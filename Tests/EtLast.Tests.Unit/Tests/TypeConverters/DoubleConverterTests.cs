namespace FizzCode.EtLast.Tests.Unit.TypeConverters;

[TestClass]
public class DoubleConverterTests
{
    [TestMethod]
    [DataRow("1", 1d)]
    [DataRow("1234.456", 1234.456d)]
    [DataRow("12345678901234567890", 1.2345678901234567E+19)]
    [DataRow((sbyte)77, 77d)]
    [DataRow((byte)77, 77d)]
    [DataRow((short)77, 77d)]
    [DataRow((ushort)77, 77d)]
    [DataRow(77, 77d)]
    [DataRow((uint)77, 77d)]
    [DataRow((long)77, 77d)]
    [DataRow((ulong)77, 77d)]
    [DataRow(3f, 3d)]
    [DataRow(3.12d, 3.12d)]
    public void DoubleConverter(object input, double? expected)
    {
        var converter = new DoubleConverter();
        var result = converter.Convert(input);
        Assert.AreEqual(expected, result);
    }

    [TestMethod]
    public void DoubleConverterFromDecimal()
    {
        var converter = new DoubleConverter();
        var result = converter.Convert(4m / 5m);
        Assert.AreEqual(Convert.ToDouble(4m / 5m), result);
    }

    [TestMethod]
    [DataRow("1.234", 1.234d)]
    [DataRow("-1.234", -1.234d)]
    [DataRow("1,234", 1.234d, "hu-HU")]
    [DataRow("+1,234", 1.234d, "hu-HU")]
    [DataRow(" +1,234  ", 1.234d, "hu-HU")]
    [DataRow("+ 1,234", null, "hu-HU")]
    [DataRow("1   234 456,2 ", 1234456.2d, "hu-HU")]
    [DataRow("-1 234 456,2", -1234456.2d, "hu-HU")]
    [DataRow("- 1 234 456,2", null, "hu-HU")]
    [DataRow("1.234", 1.234d, "hu-HU")]
    [DataRow("1,234,456.2", 1234456.2, "hu-HU")]
    [DataRow("123,2", 1232d, DisplayName = "MisrecognizedThousandSeparator")]
    [DataRow("123,2", 123.2d, "hu-HU")]
    [DataRow("1 234 456,2", null)]
    [DataRow("123.2", 123.2d, "en-US")]
    [DataRow("1,234,456.2", 1234456.2d, "en-US")]
    [DataRow("1234456.2", 1234456.2d, "en-US")]
    [DataRow("1.234.456,2", null, "en-US")]
    public void DoubleConverterAuto(string input, double? expected, string locale = null)
    {
        var converter = new DoubleConverterAuto(locale == null ? CultureInfo.InvariantCulture : new CultureInfo(locale));
        var result = converter.Convert(input);
        Assert.AreEqual(expected, result);
    }
}
