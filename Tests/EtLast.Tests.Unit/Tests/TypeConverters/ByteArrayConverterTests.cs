namespace FizzCode.EtLast.Tests.Unit.TypeConverters;

[TestClass]
public class ByteArrayConverterTests
{
    [TestMethod]
    public void FromByteArray()
    {
        var converter = new ByteArrayConverter();
        var result = converter.Convert(new byte[] { 55, 66, 77 });
        Assert.AreEqual(3, (result as byte[])?.Length);
        Assert.AreEqual(55, (result as byte[])[0]);
        Assert.AreEqual(66, (result as byte[])[1]);
        Assert.AreEqual(77, (result as byte[])[2]);
    }

    [TestMethod]
    public void FromString()
    {
        var originalValue = new byte[] { 10, 57, 99, 122, 199 };
        var base64Value = Convert.ToBase64String(originalValue);
        var textBuilder = new TextBuilder();
        foreach (var @char in base64Value)
        {
            textBuilder.Append(@char);
        }
        var converter = new ByteArrayConverter();
        var result = converter.Convert(textBuilder);

        Assert.AreEqual(5, (result as byte[])?.Length);
        Assert.AreEqual(10, (result as byte[])[0]);
        Assert.AreEqual(57, (result as byte[])[1]);
        Assert.AreEqual(99, (result as byte[])[2]);
        Assert.AreEqual(122, (result as byte[])[3]);
        Assert.AreEqual(199, (result as byte[])[4]);
    }
}