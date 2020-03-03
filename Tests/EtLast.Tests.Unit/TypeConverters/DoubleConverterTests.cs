namespace FizzCode.EtLast.Tests.Unit.TypeConverters
{
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class DoubleConverterTests
    {
        [TestMethod]
        public void InvString()
        {
            var converter = new DoubleConverter();
            var result = converter.Convert("1.234");
            Assert.AreEqual(1.234d, result);
        }

        [TestMethod]
        public void InvStringThousands()
        {
            var converter = new DoubleConverter();
            var result = converter.Convert("1234.456");
            Assert.AreEqual(1234.456d, result);
        }

        [TestMethod]
        public void FromSByte()
        {
            var converter = new DoubleConverter();
            var result = converter.Convert((sbyte)77);
            Assert.AreEqual(77d, result);
        }

        [TestMethod]
        public void FromByte()
        {
            var converter = new DoubleConverter();
            var result = converter.Convert((byte)77);
            Assert.AreEqual(77d, result);
        }

        [TestMethod]
        public void FromShort()
        {
            var converter = new DoubleConverter();
            var result = converter.Convert((short)77);
            Assert.AreEqual(77d, result);
        }

        [TestMethod]
        public void FromUShort()
        {
            var converter = new DoubleConverter();
            var result = converter.Convert((ushort)77);
            Assert.AreEqual(77d, result);
        }

        [TestMethod]
        public void FromInt()
        {
            var converter = new DoubleConverter();
            var result = converter.Convert(77);
            Assert.AreEqual(77d, result);
        }

        [TestMethod]
        public void FromUInt()
        {
            var converter = new DoubleConverter();
            var result = converter.Convert(77u);
            Assert.AreEqual(77d, result);
        }

        [TestMethod]
        public void FromLong()
        {
            var converter = new DoubleConverter();
            var result = converter.Convert(long.MaxValue);
            Assert.AreEqual((double)long.MaxValue, result);
        }

        [TestMethod]
        public void FromULong()
        {
            var converter = new DoubleConverter();
            var result = converter.Convert(ulong.MaxValue);
            Assert.AreEqual((double)ulong.MaxValue, result);
        }

        [TestMethod]
        public void FromFloat()
        {
            var converter = new DoubleConverter();
            var result = converter.Convert(4f / 5f);
            Assert.AreEqual((double)(4f / 5f), result);
        }

        [TestMethod]
        public void FromDouble()
        {
            var converter = new DoubleConverter();
            var result = converter.Convert(4d / 5d);
            Assert.AreEqual(4d / 5d, result);
        }

        [TestMethod]
        public void FromDecimal()
        {
            var converter = new DoubleConverter();
            var result = converter.Convert(4m / 5m);
            Assert.AreEqual(System.Convert.ToDouble(4m / 5m), result);
        }

        [TestMethod]
        public void FromDecimalBig()
        {
            var converter = new DoubleConverter();
            var result = converter.Convert(decimal.MaxValue);
            Assert.AreEqual((double)decimal.MaxValue, result);
        }
    }
}