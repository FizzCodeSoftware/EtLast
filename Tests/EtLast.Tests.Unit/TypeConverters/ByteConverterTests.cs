namespace FizzCode.EtLast.Tests.Unit.TypeConverters
{
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class ByteConverterTests
    {
        [TestMethod]
        public void InvString()
        {
            var converter = new ByteConverter();
            var result = converter.Convert("1");
            Assert.AreEqual((byte)1, result);
        }

        [TestMethod]
        public void InvStringBigger()
        {
            var converter = new ByteConverter();
            var result = converter.Convert("123");
            Assert.AreEqual((byte)123, result);
        }

        [TestMethod]
        public void InvStringTooSmall()
        {
            var converter = new ByteConverter();
            var result = converter.Convert("-1");
            Assert.AreEqual(null, result);
        }

        [TestMethod]
        public void InvStringTooBig()
        {
            var converter = new ByteConverter();
            var result = converter.Convert("1234");
            Assert.AreEqual(null, result);
        }

        [TestMethod]
        public void FromSByte()
        {
            var converter = new ByteConverter();
            var result = converter.Convert((sbyte)77);
            Assert.AreEqual((byte)77, result);
        }

        [TestMethod]
        public void FromSByteTooSmall()
        {
            var converter = new ByteConverter();
            var result = converter.Convert(sbyte.MinValue);
            Assert.AreEqual(null, result);
        }

        [TestMethod]
        public void FromByte()
        {
            var converter = new ByteConverter();
            var result = converter.Convert((byte)77);
            Assert.AreEqual((byte)77, result);
        }

        [TestMethod]
        public void FromShort()
        {
            var converter = new ByteConverter();
            var result = converter.Convert((short)77);
            Assert.AreEqual((byte)77, result);
        }

        [TestMethod]
        public void FromShortTooBig()
        {
            var converter = new ByteConverter();
            var result = converter.Convert(short.MaxValue);
            Assert.AreEqual(null, result);
        }

        [TestMethod]
        public void FromShortTooSmall()
        {
            var converter = new ByteConverter();
            var result = converter.Convert(short.MinValue);
            Assert.AreEqual(null, result);
        }

        [TestMethod]
        public void FromUShort()
        {
            var converter = new ByteConverter();
            var result = converter.Convert((ushort)77);
            Assert.AreEqual((byte)77, result);
        }

        [TestMethod]
        public void FromUShortTooBig()
        {
            var converter = new ByteConverter();
            var result = converter.Convert(ushort.MaxValue);
            Assert.AreEqual(null, result);
        }

        [TestMethod]
        public void FromInt()
        {
            var converter = new ByteConverter();
            var result = converter.Convert(77);
            Assert.AreEqual((byte)77, result);
        }

        [TestMethod]
        public void FromIntTooBig()
        {
            var converter = new ByteConverter();
            var result = converter.Convert(int.MaxValue);
            Assert.AreEqual(null, result);
        }

        [TestMethod]
        public void FromIntTooSmall()
        {
            var converter = new ByteConverter();
            var result = converter.Convert(int.MinValue);
            Assert.AreEqual(null, result);
        }

        [TestMethod]
        public void FromUInt()
        {
            var converter = new ByteConverter();
            var result = converter.Convert(77u);
            Assert.AreEqual((byte)77, result);
        }

        [TestMethod]
        public void FromUIntTooBig()
        {
            var converter = new ByteConverter();
            var result = converter.Convert(uint.MaxValue);
            Assert.AreEqual(null, result);
        }

        [TestMethod]
        public void FromLong()
        {
            var converter = new ByteConverter();
            var result = converter.Convert(77L);
            Assert.AreEqual((byte)77, result);
        }

        [TestMethod]
        public void FromLongTooBig()
        {
            var converter = new ByteConverter();
            var result = converter.Convert(long.MaxValue);
            Assert.AreEqual(null, result);
        }

        [TestMethod]
        public void FromLongTooSmall()
        {
            var converter = new ByteConverter();
            var result = converter.Convert(long.MinValue);
            Assert.AreEqual(null, result);
        }

        [TestMethod]
        public void FromULong()
        {
            var converter = new ByteConverter();
            var result = converter.Convert(77ul);
            Assert.AreEqual((byte)77, result);
        }

        [TestMethod]
        public void FromULongTooBig()
        {
            var converter = new ByteConverter();
            var result = converter.Convert(ulong.MaxValue);
            Assert.AreEqual(null, result);
        }

        [TestMethod]
        public void FromDouble()
        {
            var converter = new ByteConverter();
            var result = converter.Convert(8d / 3d);
            Assert.AreEqual(System.Convert.ToByte(8d / 3d), result);
        }

        [TestMethod]
        public void FromDoubleTooBig()
        {
            var converter = new ByteConverter();
            var result = converter.Convert(double.MaxValue);
            Assert.AreEqual(null, result);
        }

        [TestMethod]
        public void FromDoubleTooSmall()
        {
            var converter = new ByteConverter();
            var result = converter.Convert(double.MinValue);
            Assert.AreEqual(null, result);
        }

        [TestMethod]
        public void FromFloat()
        {
            var converter = new ByteConverter();
            var result = converter.Convert(4f / 3f);
            Assert.AreEqual(System.Convert.ToByte(4f / 3f), result);
        }

        [TestMethod]
        public void FromFloatTooBig()
        {
            var converter = new ByteConverter();
            var result = converter.Convert(float.MaxValue);
            Assert.AreEqual(null, result);
        }

        [TestMethod]
        public void FromFloatTooSmall()
        {
            var converter = new ByteConverter();
            var result = converter.Convert(float.MinValue);
            Assert.AreEqual(null, result);
        }

        [TestMethod]
        public void FromDecimal()
        {
            var converter = new ByteConverter();
            var result = converter.Convert(4m / 3m);
            Assert.AreEqual(System.Convert.ToByte(4m / 3m), result);
        }

        [TestMethod]
        public void FromDecimalTooBig()
        {
            var converter = new ByteConverter();
            var result = converter.Convert(decimal.MaxValue);
            Assert.AreEqual(null, result);
        }

        [TestMethod]
        public void FromDecimalTooSmall()
        {
            var converter = new ByteConverter();
            var result = converter.Convert(decimal.MinValue);
            Assert.AreEqual(null, result);
        }
    }
}