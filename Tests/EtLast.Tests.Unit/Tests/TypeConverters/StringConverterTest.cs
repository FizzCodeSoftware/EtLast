namespace FizzCode.EtLast.Tests.Unit.TypeConverters;

[TestClass]
public class StringConverterTest
{
    [TestMethod]
    [DataRow("fizzcode", "fizzcode")]
    [DataRow(71.11d, "71.11")]
    [DataRow(71.11d, "71,11", "hu-HU")]
    public void Default(object input, string output, string locale = null)
    {
        var converter = new StringConverter(locale != null ? new CultureInfo(locale) : null);
        var result = converter.Convert(input);
        Assert.AreEqual(output, result);
    }
}