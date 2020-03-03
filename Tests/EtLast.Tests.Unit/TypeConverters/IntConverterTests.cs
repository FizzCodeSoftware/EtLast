namespace FizzCode.EtLast.Tests.Unit.TypeConverters
{
    using System.Globalization;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class IntConverterTests
    {
        [TestMethod]
        [DataRow("1", 1)]
        [DataRow("1234", 1234)]
        [DataRow("12345678901234567890", null)]
        [DataRow((sbyte)77, 77)]
        [DataRow((byte)77, 77)]
        [DataRow((short)77, 77)]
        [DataRow((ushort)77, 77)]
        [DataRow(77, 77)]
        [DataRow((uint)77, 77)]
        [DataRow(uint.MaxValue, null)]
        [DataRow((long)77, 77)]
        [DataRow((ulong)77, 77)]
        [DataRow(long.MaxValue, null)]
        [DataRow(long.MinValue, null)]
        [DataRow(ulong.MaxValue, null)]
        [DataRow(77.5f, 78)]
        [DataRow(78.5f, 78)]
        [DataRow(79.5f, 80)]
        [DataRow(float.MaxValue, null)]
        [DataRow(float.MinValue, null)]
        [DataRow(77.5d, 78)]
        [DataRow(78.5d, 78)]
        [DataRow(79.5d, 80)]
        [DataRow(double.MaxValue, null)]
        [DataRow(double.MinValue, null)]
        public void IntConverter(object input, int? expected)
        {
            var converter = new IntConverter();
            var result = converter.Convert(input);
            Assert.AreEqual(expected, result);
        }

        [TestMethod]
        public void IntConverterFromDecimal()
        {
            var converter = new IntConverter();
            var result = converter.Convert(77.5m);
            Assert.AreEqual(78, result);
        }

        [TestMethod]
        public void IntConverterFromDecimalTooBig()
        {
            var converter = new IntConverter();
            var result = converter.Convert(decimal.MaxValue);
            Assert.AreEqual(null, result);
        }

        [TestMethod]
        public void IntConverterFromDecimalTooSmall()
        {
            var converter = new IntConverter();
            var result = converter.Convert(decimal.MinValue);
            Assert.AreEqual(null, result);
        }

        [TestMethod]
        [DataRow("1234", 1234)]
        [DataRow("-1234", -1234)]
        [DataRow("1 234", 1234, "hu-HU")]
        [DataRow("-1 234", -1234, "hu-HU")]
        [DataRow("+1 234", 1234, "hu-HU")]
        [DataRow(" +1 234  ", 1234, "hu-HU")]
        [DataRow(" + 1 234  ", null, "hu-HU")]
        [DataRow("1   234 456", 1234456, "hu-HU")]
        [DataRow("-1 234 456", -1234456, "hu-HU")]
        [DataRow("- 1 234 456", null, "hu-HU")]
        [DataRow("1,234", 1234, "hu-HU")]
        [DataRow("1,234,456", 1234456, "hu-HU")]
        [DataRow("1 234 456,2", null)]
        [DataRow("123", 123, "en-US")]
        [DataRow("1,234,456", 1234456, "en-US")]
        [DataRow("1234456", 1234456, "en-US")]
        [DataRow("1.234.456", null, "en-US")]
        public void IntConverterAuto(string input, int? expected, string locale = null)
        {
            var converter = new IntConverterAuto(locale == null ? CultureInfo.InvariantCulture : new CultureInfo(locale));
            var result = converter.Convert(input);
            Assert.AreEqual(expected, result);
        }
    }
}