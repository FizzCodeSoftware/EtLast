namespace FizzCode.EtLast.Tests.Unit
{
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class StringConverterTest
    {
        [TestMethod]
        public void ConverObject()
        {
            var stringConverter = new StringConverter();
            object value = (object)"fizzcode";
            var result = stringConverter.Convert(value);

            Assert.AreEqual("fizzcode", result);
        }
    }
}