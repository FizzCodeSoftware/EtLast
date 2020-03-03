namespace FizzCode.EtLast.Tests.Unit.TypeConverters
{
    using System.Globalization;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class LongConverterTests
    {
        [TestMethod]
        [DataRow("1", 1L)]
        [DataRow("1234", 1234L)]
        [DataRow("12345678901234567890", null)]
        [DataRow((sbyte)77, 77L)]
        [DataRow((byte)77, 77L)]
        [DataRow((short)77, 77L)]
        [DataRow((ushort)77, 77L)]
        [DataRow(77, 77L)]
        [DataRow((uint)77, 77L)]
        [DataRow((long)77, 77L)]
        [DataRow((ulong)77, 77L)]
        [DataRow(ulong.MaxValue, null)]
        [DataRow(77.5f, 78L)]
        [DataRow(78.5f, 78L)]
        [DataRow(79.5f, 80L)]
        [DataRow(77.5d, 78L)]
        [DataRow(78.5d, 78L)]
        [DataRow(79.5d, 80L)]
        [DataRow(double.MaxValue, null)]
        [DataRow(double.MinValue, null)]
        public void LongConverter(object input, long? expected)
        {
            var converter = new LongConverter();
            var result = converter.Convert(input);
            Assert.AreEqual(expected, result);
        }

        [TestMethod]
        public void LongConverterFromDecimal()
        {
            var converter = new LongConverter();
            var result = converter.Convert(77.5);
            Assert.AreEqual(78L, result);
        }

        [TestMethod]
        [DataRow("1234", 1234L)]
        [DataRow("-1234", -1234L)]
        [DataRow("1 234", 1234L, "hu-HU")]
        [DataRow("-1 234", -1234L, "hu-HU")]
        [DataRow("+1 234", 1234L, "hu-HU")]
        [DataRow(" +1 234  ", 1234L, "hu-HU")]
        [DataRow(" + 1 234  ", null, "hu-HU")]
        [DataRow("1   234 456", 1234456L, "hu-HU")]
        [DataRow("-1 234 456", -1234456L, "hu-HU")]
        [DataRow("- 1 234 456", null, "hu-HU")]
        [DataRow("1,234", 1234L, "hu-HU")]
        [DataRow("1,234,456", 1234456L, "hu-HU")]
        [DataRow("1 234 456,2", null)]
        [DataRow("123", 123L, "en-US")]
        [DataRow("1,234,456", 1234456L, "en-US")]
        [DataRow("1234456", 1234456L, "en-US")]
        [DataRow("1.234.456", null, "en-US")]
        public void LongConverterAuto(string input, long? expected, string locale = null)
        {
            var converter = new LongConverterAuto(locale == null ? CultureInfo.InvariantCulture : new CultureInfo(locale));
            var result = converter.Convert(input);
            Assert.AreEqual(expected, result);
        }
    }
}