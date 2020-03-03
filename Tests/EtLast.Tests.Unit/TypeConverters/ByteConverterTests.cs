namespace FizzCode.EtLast.Tests.Unit.TypeConverters
{
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class ByteConverterTests
    {
        [TestMethod]
        [DataRow("1", (byte)1)]
        [DataRow("123", (byte)123)]
        [DataRow("-1", null)]
        [DataRow("1234", null)]
        [DataRow((sbyte)77, (byte)77)]
        [DataRow(sbyte.MinValue, null)]
        [DataRow((byte)77, (byte)77)]
        [DataRow((short)77, (byte)77)]
        [DataRow(short.MaxValue, null)]
        [DataRow(short.MinValue, null)]
        [DataRow((ushort)77, (byte)77)]
        [DataRow(ushort.MaxValue, null)]
        [DataRow(77, (byte)77)]
        [DataRow(int.MaxValue, null)]
        [DataRow(int.MinValue, null)]
        [DataRow(77u, (byte)77)]
        [DataRow(uint.MaxValue, null)]
        [DataRow(77L, (byte)77)]
        [DataRow(long.MaxValue, null)]
        [DataRow(long.MinValue, null)]
        [DataRow(77ul, (byte)77)]
        [DataRow(ulong.MaxValue, null)]
        [DataRow(77.5f, (byte)78)]
        [DataRow(78.5f, (byte)78)]
        [DataRow(79.5f, (byte)80)]
        [DataRow(float.MaxValue, null)]
        [DataRow(float.MinValue, null)]
        [DataRow(77.5d, (byte)78)]
        [DataRow(78.5d, (byte)78)]
        [DataRow(79.5d, (byte)80)]
        [DataRow(double.MaxValue, null)]
        [DataRow(double.MinValue, null)]
        public void ByteConverter(object input, byte? expected)
        {
            var converter = new ByteConverter();
            var result = converter.Convert(input);
            Assert.AreEqual(expected, result);
        }

        [TestMethod]
        public void ByteConverterFromDecimal()
        {
            var converter = new ByteConverter();
            var result = converter.Convert(76.5m);
            Assert.AreEqual((byte)76, result);
        }

        [TestMethod]
        public void ByteConverterFromDecimalTooBig()
        {
            var converter = new ByteConverter();
            var result = converter.Convert(decimal.MaxValue);
            Assert.AreEqual(null, result);
        }

        [TestMethod]
        public void ByteConverterFromDecimalTooSmall()
        {
            var converter = new ByteConverter();
            var result = converter.Convert(decimal.MinValue);
            Assert.AreEqual(null, result);
        }
    }
}