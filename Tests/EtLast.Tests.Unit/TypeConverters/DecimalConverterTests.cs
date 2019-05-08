namespace FizzCode.EtLast.Tests.Unit.TypeConverters
{
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class DecimalConverterTest
    {
        [TestMethod]
        public void ConverFromStringWithDecimalPoint()
        {
            // CultureInfo.InvariantCulture uses . as a decimal separator, and , as a thousands separator. 
            var converter = new DecimalConverter(true);
            const string value = "1.234";
            var result = converter.Convert(value);

            Assert.AreEqual(1.234m, result);
        }
    }
}