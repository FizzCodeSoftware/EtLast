namespace FizzCode.EtLast.Tests.Unit.TypeConverters
{
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class StringConverterTest
    {
        [TestMethod]
        public void ConverObject()
        {
            var converter = new StringConverter();
            var value = (object)"fizzcode";
            var result = converter.Convert(value);

            Assert.AreEqual("fizzcode", result);
        }
    }
}