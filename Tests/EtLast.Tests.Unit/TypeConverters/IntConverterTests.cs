namespace FizzCode.EtLast.Tests.Unit.TypeConverters
{
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class IntConverterTests
    {
        [TestMethod]
        public void InvString()
        {
            var converter = new IntConverter();
            var result = converter.Convert("1");
            Assert.AreEqual(1, result);
        }

        [TestMethod]
        public void InvStringThousands()
        {
            var converter = new IntConverter();
            var result = converter.Convert("1234");
            Assert.AreEqual(1234, result);
        }

        [TestMethod]
        public void InvStringTooBig()
        {
            var converter = new IntConverter();
            var result = converter.Convert("123456789012");
            Assert.AreEqual(null, result);
        }

        [TestMethod]
        public void FromSByte()
        {
            var converter = new IntConverter();
            var result = converter.Convert((sbyte)77);
            Assert.AreEqual(77, result);
        }

        [TestMethod]
        public void FromByte()
        {
            var converter = new IntConverter();
            var result = converter.Convert((byte)77);
            Assert.AreEqual(77, result);
        }

        [TestMethod]
        public void FromShort()
        {
            var converter = new IntConverter();
            var result = converter.Convert((short)77);
            Assert.AreEqual(77, result);
        }

        [TestMethod]
        public void FromUShort()
        {
            var converter = new IntConverter();
            var result = converter.Convert((ushort)77);
            Assert.AreEqual(77, result);
        }

        [TestMethod]
        public void FromInt()
        {
            var converter = new IntConverter();
            var result = converter.Convert(77);
            Assert.AreEqual(77, result);
        }

        [TestMethod]
        public void FromUInt()
        {
            var converter = new IntConverter();
            var result = converter.Convert(77u);
            Assert.AreEqual(77, result);
        }

        [TestMethod]
        public void FromUIntTooBig()
        {
            var converter = new IntConverter();
            var result = converter.Convert(uint.MaxValue);
            Assert.AreEqual(null, result);
        }

        [TestMethod]
        public void FromLong()
        {
            var converter = new IntConverter();
            var result = converter.Convert((long)int.MaxValue);
            Assert.AreEqual(int.MaxValue, result);
        }

        [TestMethod]
        public void FromLongTooBig()
        {
            var converter = new IntConverter();
            var result = converter.Convert(long.MaxValue);
            Assert.AreEqual(null, result);
        }

        [TestMethod]
        public void FromLongTooSmall()
        {
            var converter = new IntConverter();
            var result = converter.Convert(long.MinValue);
            Assert.AreEqual(null, result);
        }

        [TestMethod]
        public void FromULong()
        {
            var converter = new IntConverter();
            var result = converter.Convert(77ul);
            Assert.AreEqual(77, result);
        }

        [TestMethod]
        public void FromULongTooBig()
        {
            var converter = new IntConverter();
            var result = converter.Convert(ulong.MaxValue);
            Assert.AreEqual(null, result);
        }

        [TestMethod]
        public void FromDouble()
        {
            var converter = new IntConverter();
            var result = converter.Convert(4d / 5d);
            Assert.AreEqual(System.Convert.ToInt32(4d / 5d), result);
        }

        [TestMethod]
        public void FromDoubleTooBig()
        {
            var converter = new IntConverter();
            var result = converter.Convert(double.MaxValue);
            Assert.AreEqual(null, result);
        }

        [TestMethod]
        public void FromDoubleTooSmall()
        {
            var converter = new IntConverter();
            var result = converter.Convert(double.MinValue);
            Assert.AreEqual(null, result);
        }

        [TestMethod]
        public void FromFloat()
        {
            var converter = new IntConverter();
            var result = converter.Convert(4f / 5f);
            Assert.AreEqual(System.Convert.ToInt32(4f / 5f), result);
        }

        [TestMethod]
        public void FromFloatTooBig()
        {
            var converter = new IntConverter();
            var result = converter.Convert(float.MaxValue);
            Assert.AreEqual(null, result);
        }

        [TestMethod]
        public void FromFloatTooSmall()
        {
            var converter = new IntConverter();
            var result = converter.Convert(float.MinValue);
            Assert.AreEqual(null, result);
        }

        [TestMethod]
        public void FromDecimal()
        {
            var converter = new IntConverter();
            var result = converter.Convert(4m / 5m);
            Assert.AreEqual(System.Convert.ToInt32(4m / 5m), result);
        }

        [TestMethod]
        public void FromDecimalTooBig()
        {
            var converter = new IntConverter();
            var result = converter.Convert(decimal.MaxValue);
            Assert.AreEqual(null, result);
        }

        [TestMethod]
        public void FromDecimalTooSmall()
        {
            var converter = new IntConverter();
            var result = converter.Convert(decimal.MinValue);
            Assert.AreEqual(null, result);
        }
    }
}