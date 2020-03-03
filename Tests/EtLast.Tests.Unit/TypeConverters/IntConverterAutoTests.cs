namespace FizzCode.EtLast.Tests.Unit.TypeConverters
{
    using System.Globalization;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class IntConverterAutoTests
    {
        [TestMethod]
        public void InvInv()
        {
            var converter = new IntConverterAuto(CultureInfo.InvariantCulture);
            var result = converter.Convert("1234");
            Assert.AreEqual(1234, result);
        }

        [TestMethod]
        public void InvInvNeg()
        {
            var converter = new IntConverterAuto(CultureInfo.InvariantCulture);
            var result = converter.Convert("-1234");
            Assert.AreEqual(-1234, result);
        }

        [TestMethod]
        public void HuHu()
        {
            var converter = new IntConverterAuto(new CultureInfo("hu-HU"));
            var result = converter.Convert("1 234");
            Assert.AreEqual(1234, result);
        }

        [TestMethod]
        public void HuHuPos()
        {
            var converter = new IntConverterAuto(new CultureInfo("hu-HU"));
            var result = converter.Convert("+1 234");
            Assert.AreEqual(1234, result);
        }

        [TestMethod]
        public void HuHuPosWhiteSpace()
        {
            var converter = new IntConverterAuto(new CultureInfo("hu-HU"));
            var result = converter.Convert(" +1 234  ");
            Assert.AreEqual(1234, result);
        }

        [TestMethod]
        public void HuHuPosBrokenWhiteSpace()
        {
            var converter = new IntConverterAuto(new CultureInfo("hu-HU"));
            var result = converter.Convert("+ 1,234");
            Assert.AreEqual(null, result);
        }

        [TestMethod]
        public void HuHuThousandsWhiteSpace()
        {
            var converter = new IntConverterAuto(new CultureInfo("hu-HU"));
            var result = converter.Convert("1   234 456 ");
            Assert.AreEqual(1234456, result);
        }

        [TestMethod]
        public void HuHuThousandsNegWhiteSpace()
        {
            var converter = new IntConverterAuto(new CultureInfo("hu-HU"));
            var result = converter.Convert("-1 234 456");
            Assert.AreEqual(-1234456, result);
        }

        [TestMethod]
        public void HuHuThousandsNegBrokenWhiteSpace()
        {
            var converter = new IntConverterAuto(new CultureInfo("hu-HU"));
            var result = converter.Convert("- 1 234 456");
            Assert.AreEqual(null, result);
        }

        [TestMethod]
        public void HuInvFallback()
        {
            var converter = new IntConverterAuto(new CultureInfo("hu-HU"));
            var result = converter.Convert("1,234");
            Assert.AreEqual(1234, result);
        }

        [TestMethod]
        public void HuEnStringWithThousandsFallbackToInvariant()
        {
            var converter = new IntConverterAuto(new CultureInfo("hu-HU"));
            var result = converter.Convert("1,234,456");
            Assert.AreEqual(1234456, result);
        }

        [TestMethod]
        public void InvHuStringWithThousandsBroken()
        {
            var converter = new IntConverterAuto(CultureInfo.InvariantCulture);
            var result = converter.Convert("1 234 456,2");
            Assert.AreEqual(null, result);
        }

        [TestMethod]
        public void UsUs()
        {
            var converter = new IntConverterAuto(new CultureInfo("en-US"));
            var result = converter.Convert("123");
            Assert.AreEqual(123, result);
        }

        [TestMethod]
        public void UsUsThousands()
        {
            var converter = new IntConverterAuto(new CultureInfo("en-US"));
            var result = converter.Convert("1,234,456");
            Assert.AreEqual(1234456, result);
        }

        [TestMethod]
        public void UsInvFallback()
        {
            var converter = new IntConverterAuto(new CultureInfo("en-US"));
            var result = converter.Convert("1234456");
            Assert.AreEqual(1234456, result);
        }

        [TestMethod]
        public void UsInvBroken()
        {
            var converter = new IntConverterAuto(new CultureInfo("en-US"));
            var result = converter.Convert("1.234.456");
            Assert.AreEqual(null, result);
        }
    }
}