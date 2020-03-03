namespace FizzCode.EtLast.Tests.Unit.TypeConverters
{
    using System;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class TimeConverterTests
    {
        [TestMethod]
        public void InvTimeSpanString()
        {
            var converter = new TimeConverter();
            var result = converter.Convert("13:14:41.410");
            Assert.AreEqual(new TimeSpan(0, 13, 14, 41, 410), result);
        }

        [TestMethod]
        public void InvTimeSpanStringWithDays()
        {
            var converter = new TimeConverter();
            var result = converter.Convert("112:13:14:41.410");
            Assert.AreEqual(new TimeSpan(112, 13, 14, 41, 410), result);
        }

        [TestMethod]
        public void InvDateTimeString()
        {
            var converter = new TimeConverter();
            var result = converter.Convert("2020.02.02 13:14:41.410");
            Assert.AreEqual(new TimeSpan(0, 13, 14, 41, 410), result);
        }

        [TestMethod]
        public void FromStringWithOneNumber()
        {
            var converter = new TimeConverter();
            var result = converter.Convert(" 134578  ");
            Assert.AreEqual(new TimeSpan(134578, 0, 0, 0), result);
        }

        [TestMethod]
        public void FromStringWithOneNumberSmaller()
        {
            var converter = new TimeConverter();
            var result = converter.Convert(" 134  ");
            Assert.AreEqual(new TimeSpan(134, 0, 0, 0), result);
        }

        [TestMethod]
        public void FromTimeSpan()
        {
            var converter = new TimeConverter();
            var result = converter.Convert(new TimeSpan(134578));
            Assert.AreEqual(new TimeSpan(134578), result);
        }

        [TestMethod]
        public void FromTimeDateTime()
        {
            var converter = new TimeConverter();
            var result = converter.Convert(new DateTime(2010, 5, 12, 13, 14, 41, 410));
            Assert.AreEqual(new DateTime(2010, 5, 12, 13, 14, 41, 410).TimeOfDay, result);
        }
    }
}