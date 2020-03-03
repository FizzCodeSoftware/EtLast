namespace FizzCode.EtLast.Tests.Unit.TypeConverters
{
    using System;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class DateConverterTests
    {
        [TestMethod]
        public void InvDateTimeString()
        {
            var converter = new DateConverter();
            var result = converter.Convert("2020.05.02 13:14:41.410");
            Assert.AreEqual(new DateTime(2020, 5, 2, 0, 0, 0, 0), result);
        }

        [TestMethod]
        public void InvDateTimeStringWithoutYear()
        {
            var converter = new DateConverter();
            var result = converter.Convert("5.2 13:14:41.410");
            Assert.AreEqual(null, result);
        }

        [TestMethod]
        public void InvDateTimeStringWithoutYear2()
        {
            var converter = new DateConverter();
            var result = converter.Convert("5.2");
            Assert.AreEqual(new DateTime(DateTime.Now.Year, 5, 2), result);
        }

        [TestMethod]
        public void InvDateTimeStringOnlyDateWithPeriodEnd()
        {
            var converter = new DateConverter();
            var result = converter.Convert("2020.5.2.");
            Assert.AreEqual(new DateTime(2020, 5, 2), result);
        }

        [TestMethod]
        public void FromDateTime()
        {
            var converter = new DateConverter();
            var result = converter.Convert(new DateTime(2020, 5, 2, 13, 14, 41, 410));
            Assert.AreEqual(new DateTime(2020, 5, 2, 0, 0, 0, 0), result);
        }

        [TestMethod]
        public void FromEpochDouble()
        {
            var converter = new DateConverter() { EpochDate = new DateTime(1900, 1, 1) };
            var result = converter.Convert(12.5d);
            Assert.AreEqual(new DateTime(1900, 1, 13, 0, 0, 0, 0), result);
        }

        [TestMethod]
        public void FromEpochInvString()
        {
            var converter = new DateConverter() { EpochDate = new DateTime(1900, 1, 1) };
            var result = converter.Convert("1215.5");
            Assert.AreEqual(new DateTime(1903, 5, 1, 0, 0, 0, 0), result);
        }
    }
}