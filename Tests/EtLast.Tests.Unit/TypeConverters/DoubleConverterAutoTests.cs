namespace FizzCode.EtLast.Tests.Unit.TypeConverters
{
    using System.Globalization;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class DoubleConverterAutoTests
    {
        [TestMethod]
        public void StringWithDecimalPoint()
        {
            var numberFormatInfo = CultureInfo.InvariantCulture.NumberFormat;

            var converter = new DoubleConverterAuto(numberFormatInfo, NumberStyles.Number, true);
            string value = "1.234";
            var result = converter.Convert(value);

            Assert.AreEqual(1.234D, result);
        }

        [TestMethod]
        public void BadStringWithDecimalPoint()
        {
            var numberFormatInfo = new CultureInfo("hu-HU");

            var converter = new DoubleConverterAuto(numberFormatInfo, NumberStyles.Number, true);
            string value = "1.234";
            var result = converter.Convert(value);

            // Failed conversion expected, defaulting to null
            Assert.AreEqual(null, result);
        }

        [TestMethod]
        public void StringWithDecimalComma()
        {
            var numberFormatInfo = new CultureInfo("hu-HU");

            var converter = new DoubleConverterAuto(numberFormatInfo, NumberStyles.Number, true);
            string value = "1,234";
            var result = converter.Convert(value);

            Assert.AreEqual(1.234D, result);
        }
    }
}