namespace FizzCode.EtLast.Tests.Unit.TypeConverters
{
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class LongConverterTests
    {
        [TestMethod]
        public void InvString()
        {
            var converter = new LongConverter();
            var result = converter.Convert("1");
            Assert.AreEqual(1L, result);
        }

        [TestMethod]
        public void InvStringThousands()
        {
            var converter = new LongConverter();
            var result = converter.Convert("1234");
            Assert.AreEqual(1234L, result);
        }

        [TestMethod]
        public void InvStringThousandsTooBig()
        {
            var converter = new LongConverter();
            var result = converter.Convert("12345678901234567890");
            Assert.AreEqual(null, result);
        }

        [TestMethod]
        public void FromSByte()
        {
            var converter = new LongConverter();
            var result = converter.Convert((sbyte)77);
            Assert.AreEqual(77L, result);
        }

        [TestMethod]
        public void FromByte()
        {
            var converter = new LongConverter();
            var result = converter.Convert((byte)77);
            Assert.AreEqual(77L, result);
        }

        [TestMethod]
        public void FromShort()
        {
            var converter = new LongConverter();
            var result = converter.Convert((short)77);
            Assert.AreEqual(77L, result);
        }

        [TestMethod]
        public void FromUShort()
        {
            var converter = new LongConverter();
            var result = converter.Convert((ushort)77);
            Assert.AreEqual(77L, result);
        }

        [TestMethod]
        public void FromInt()
        {
            var converter = new LongConverter();
            var result = converter.Convert(77);
            Assert.AreEqual(77L, result);
        }

        [TestMethod]
        public void FromUInt()
        {
            var converter = new LongConverter();
            var result = converter.Convert(77u);
            Assert.AreEqual(77L, result);
        }

        [TestMethod]
        public void FromLong()
        {
            var converter = new LongConverter();
            var result = converter.Convert(77L);
            Assert.AreEqual(77L, result);
        }

        [TestMethod]
        public void FromULong()
        {
            var converter = new LongConverter();
            var result = converter.Convert(77ul);
            Assert.AreEqual(77L, result);
        }

        [TestMethod]
        public void FromULongTooBig()
        {
            var converter = new LongConverter();
            var result = converter.Convert(ulong.MaxValue);
            Assert.AreEqual(null, result);
        }

        [TestMethod]
        public void FromFloat()
        {
            var converter = new LongConverter();
            var result = converter.Convert(4f / 5f);
            Assert.AreEqual(System.Convert.ToInt64(4f / 5f), result);
        }

        [TestMethod]
        public void FromDouble()
        {
            var converter = new LongConverter();
            var result = converter.Convert(4d / 5d);
            Assert.AreEqual(System.Convert.ToInt64(4d / 5d), result);
        }

        [TestMethod]
        public void FromDoubleTooBig()
        {
            var converter = new LongConverter();
            var result = converter.Convert(double.MaxValue);
            Assert.AreEqual(null, result);
        }

        [TestMethod]
        public void FromDoubleTooSmall()
        {
            var converter = new LongConverter();
            var result = converter.Convert(double.MinValue);
            Assert.AreEqual(null, result);
        }

        [TestMethod]
        public void FromDecimal()
        {
            var converter = new LongConverter();
            var result = converter.Convert(4m / 5m);
            Assert.AreEqual(System.Convert.ToInt64(4m / 5m), result);
        }
    }
}