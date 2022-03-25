namespace FizzCode.EtLast.Tests.Unit.TypeConverters;

using Microsoft.VisualStudio.TestTools.UnitTesting;

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
}
