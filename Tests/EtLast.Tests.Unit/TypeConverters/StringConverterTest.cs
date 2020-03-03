namespace FizzCode.EtLast.Tests.Unit.TypeConverters
{
    using System.Globalization;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class StringConverterTest
    {
        [TestMethod]
        public void StringObject()
        {
            var converter = new StringConverter();
            var result = converter.Convert("fizzcode");
            Assert.AreEqual("fizzcode", result);
        }

        [TestMethod]
        [DataRow(71.11d, "71.11", DisplayName = "IntegerToInv")]
        [DataRow(71.11d, "71,11", "hu-HU", DisplayName = "IntegerToHu")]
        public void AreEqual(object input, string output, string locale = null)
        {
            var converter = new StringConverter(locale != null ? new CultureInfo(locale) : null);
            var result = converter.Convert(input);
            Assert.AreEqual(output, result);
        }

        [TestMethod]
        public void RemoveLineBreaksOn()
        {
            var converter = new StringConverter(new CultureInfo("hu-HU"))
            {
                RemoveLineBreaks = true,
            };
            var result = converter.Convert("s\n\rome\ntex\r\nt\nis\r\rhere");
            Assert.AreEqual("sometextishere", result);
        }

        [TestMethod]
        public void RemoveLineBreaksDefault()
        {
            var converter = new StringConverter(new CultureInfo("hu-HU"));
            var result = converter.Convert("s\n\rome\ntex\r\nt\nis\r\rhere");
            Assert.AreEqual("s\n\rome\ntex\r\nt\nis\r\rhere", result);
        }

        [TestMethod]
        public void RemoveSpacesOn()
        {
            var converter = new StringConverter(new CultureInfo("hu-HU"))
            {
                RemoveSpaces = true,
            };
            var result = converter.Convert("some te  xt");
            Assert.AreEqual("sometext", result);
        }

        [TestMethod]
        public void RemoveSpacesDefault()
        {
            var converter = new StringConverter(new CultureInfo("hu-HU"));
            var result = converter.Convert("some te  xt");
            Assert.AreEqual("some te  xt", result);
        }

        [TestMethod]
        public void TrimStartEndOn()
        {
            var converter = new StringConverter(new CultureInfo("hu-HU"))
            {
                TrimStartEnd = true,
            };
            var result = converter.Convert("  some text\n");
            Assert.AreEqual("some text", result);
        }

        [TestMethod]
        public void TrimStartEndDefault()
        {
            var converter = new StringConverter(new CultureInfo("hu-HU"));
            var result = converter.Convert("  some text\n");
            Assert.AreEqual("  some text\n", result);
        }
    }
}