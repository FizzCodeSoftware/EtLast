namespace FizzCode.EtLast.Tests.Unit
{
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class DoubleConverterTests
    {
        [TestMethod]
        public void ConverFromStringWithDecimalPoint()
        {
            // CultureInfo.InvariantCulture uses . as a decimal separator, and , as a thousands separator. 
            var converter = new DoubleConverter(true);
            string value = "1.234";
            var result = converter.Convert(value);

            Assert.AreEqual(1.234D, result);
        }
    }
}