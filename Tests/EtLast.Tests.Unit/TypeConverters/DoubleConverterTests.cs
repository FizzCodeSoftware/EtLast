namespace FizzCode.EtLast.Tests.Unit.TypeConverters
{
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class DoubleConverterTests
    {
        [TestMethod]
        public void StringWithDecimalPoint()
        {
            // CultureInfo.InvariantCulture uses . as a decimal separator, and , as a thousands separator. 
            var converter = new DoubleConverter(true);
            var value = "1.234";
            var result = converter.Convert(value);

            Assert.AreEqual(1.234D, result);
        }

        [TestMethod]
        public void BadString()
        {
            // CultureInfo.InvariantCulture uses . as a decimal separator, and , as a thousands separator. 
            var converter = new DoubleConverter(true);
            var value = "1x234";
            var result = converter.Convert(value);

            // Failed conversion expected, defaulting to null
            Assert.AreEqual(null, result);
        }
    }
}