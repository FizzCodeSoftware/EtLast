namespace FizzCode.EtLast.Tests.Unit.TypeConverters
{
    using System.Globalization;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class StringConverterTest
    {
        [TestMethod]
        [DataRow("fizzcode", "fizzcode")]
        [DataRow(71.11d, "71.11")]
        [DataRow(71.11d, "71,11", "hu-HU")]
        public void Default(object input, string output, string locale = null)
        {
            var converter = new StringConverter(locale != null ? new CultureInfo(locale) : null);
            var result = converter.Convert(input);
            Assert.AreEqual(output, result);
        }

        [TestMethod]
        [DataRow("s\n\rome\ntex\r\nt\nis\r\rhere", "sometextishere", true)]
        [DataRow("s\n\rome\ntex\r\nt\nis\r\rhere", "s\n\rome\ntex\r\nt\nis\r\rhere", false)]
        [DataRow("s\n\rome\ntex\r\nt\nis\r\rhere", "s\n\rome\ntex\r\nt\nis\r\rhere")]
        public void RemoveLineBreaks(string input, string expected, bool? option = null)
        {
            var converter = new StringConverter(new CultureInfo("hu-HU"));
            if (option != null)
                converter.RemoveLineBreaks = option.Value;

            var result = converter.Convert(input);
            Assert.AreEqual(expected, result);
        }

        [TestMethod]
        [DataRow("some te  xt", "sometext", true)]
        [DataRow("some te  xt", "some te  xt", false)]
        [DataRow("some te  xt", "some te  xt")]
        public void RemoveSpaces(string input, string expected, bool? option = null)
        {
            var converter = new StringConverter(new CultureInfo("hu-HU"));
            if (option != null)
                converter.RemoveSpaces = option.Value;

            var result = converter.Convert(input);
            Assert.AreEqual(expected, result);
        }

        [TestMethod]
        [DataRow("  some text\n", "some text", true)]
        [DataRow("  some text\n", "  some text\n", false)]
        [DataRow("  some text\n", "  some text\n")]
        public void TrimStartEnd(string input, string expected, bool? option = null)
        {
            var converter = new StringConverter(new CultureInfo("hu-HU"));
            if (option != null)
                converter.TrimStartEnd = option.Value;

            var result = converter.Convert(input);
            Assert.AreEqual(expected, result);
        }
    }
}