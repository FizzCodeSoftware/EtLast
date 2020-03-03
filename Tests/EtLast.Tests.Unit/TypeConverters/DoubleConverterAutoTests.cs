namespace FizzCode.EtLast.Tests.Unit.TypeConverters
{
    using System.Globalization;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class DoubleConverterAutoTests
    {
        [TestMethod]
        public void InvInv()
        {
            var converter = new DoubleConverterAuto(CultureInfo.InvariantCulture);
            var result = converter.Convert("1.234");
            Assert.AreEqual(1.234d, result);
        }

        [TestMethod]
        public void InvInvNeg()
        {
            var converter = new DoubleConverterAuto(CultureInfo.InvariantCulture);
            var result = converter.Convert("-1.234");
            Assert.AreEqual(-1.234d, result);
        }

        [TestMethod]
        public void HuHu()
        {
            var converter = new DoubleConverterAuto(new CultureInfo("hu-HU"));
            var result = converter.Convert("1,234");
            Assert.AreEqual(1.234d, result);
        }

        [TestMethod]
        public void HuHuPos()
        {
            var converter = new DoubleConverterAuto(new CultureInfo("hu-HU"));
            var result = converter.Convert("+1,234");
            Assert.AreEqual(1.234d, result);
        }

        [TestMethod]
        public void HuHuPosWhiteSpace()
        {
            var converter = new DoubleConverterAuto(new CultureInfo("hu-HU"));
            var result = converter.Convert(" +1,234  ");
            Assert.AreEqual(1.234d, result);
        }

        [TestMethod]
        public void HuHuPosBrokenWhiteSpace()
        {
            var converter = new DoubleConverterAuto(new CultureInfo("hu-HU"));
            var result = converter.Convert("+ 1,234");
            Assert.AreEqual(null, result);
        }

        [TestMethod]
        public void HuHuThousandsWhiteSpace()
        {
            var converter = new DoubleConverterAuto(new CultureInfo("hu-HU"));
            var result = converter.Convert("1   234 456,2 ");
            Assert.AreEqual(1234456.2d, result);
        }

        [TestMethod]
        public void HuHuThousandsNegWhiteSpace()
        {
            var converter = new DoubleConverterAuto(new CultureInfo("hu-HU"));
            var result = converter.Convert("-1 234 456,2");
            Assert.AreEqual(-1234456.2d, result);
        }

        [TestMethod]
        public void HuHuThousandsNegBrokenWhiteSpace()
        {
            var converter = new DoubleConverterAuto(new CultureInfo("hu-HU"));
            var result = converter.Convert("- 1 234 456,2");
            Assert.AreEqual(null, result);
        }

        [TestMethod]
        public void HuInvFallback()
        {
            var converter = new DoubleConverterAuto(new CultureInfo("hu-HU"));
            var result = converter.Convert("1.234");
            Assert.AreEqual(1.234d, result);
        }

        [TestMethod]
        public void HuEnStringWithThousandsFallbackToInvariant()
        {
            var converter = new DoubleConverterAuto(new CultureInfo("hu-HU"));
            var result = converter.Convert("1,234,456.2");
            Assert.AreEqual(1234456.2d, result);
        }

        [TestMethod]
        public void InvHuStringMisrecognizedThousandSeparator()
        {
            var converter = new DoubleConverterAuto(CultureInfo.InvariantCulture);
            var result = converter.Convert("123,2");
            Assert.AreEqual(1232.0d, result);
        }

        [TestMethod]
        public void InvHuStringRecognizedThousandSeparator()
        {
            var converter = new DoubleConverterAuto(new CultureInfo("hu-HU"));
            var result = converter.Convert("123,2");
            Assert.AreEqual(123.2d, result);
        }

        [TestMethod]
        public void InvHuStringWithThousandsBroken()
        {
            var converter = new DoubleConverterAuto(CultureInfo.InvariantCulture);
            var result = converter.Convert("1 234 456,2");
            Assert.AreEqual(null, result);
        }

        [TestMethod]
        public void UsUs()
        {
            var converter = new DoubleConverterAuto(new CultureInfo("en-US"));
            var result = converter.Convert("123.2");
            Assert.AreEqual(123.2d, result);
        }

        [TestMethod]
        public void UsUsThousands()
        {
            var converter = new DoubleConverterAuto(new CultureInfo("en-US"));
            var result = converter.Convert("1,234,456.2");
            Assert.AreEqual(1234456.2d, result);
        }

        [TestMethod]
        public void UsInvFallback()
        {
            var converter = new DoubleConverterAuto(new CultureInfo("en-US"));
            var result = converter.Convert("1234456.2");
            Assert.AreEqual(1234456.2d, result);
        }

        [TestMethod]
        public void UsInvBroken()
        {
            var converter = new DoubleConverterAuto(new CultureInfo("en-US"));
            var result = converter.Convert("1.234.456,2");
            Assert.AreEqual(null, result);
        }
    }
}