namespace FizzCode.EtLast.Tests.Unit.TypeConverters;

using System;
using System.Globalization;
using Microsoft.VisualStudio.TestTools.UnitTesting;

[TestClass]
public class TimeConverterAutoTests
{
    [TestMethod]
    public void Hu()
    {
        var converter = new TimeConverterAuto(new CultureInfo("hu-HU"));
        var result = converter.Convert(" 13:14:41.410 ");
        Assert.AreEqual(new TimeSpan(0, 13, 14, 41, 410), result);
    }

    [TestMethod]
    public void HuFallBackToDateTimeParser()
    {
        var converter = new TimeConverterAuto(new CultureInfo("hu-HU"));
        var result = converter.Convert("13: 14:41.410");
        Assert.AreEqual(new TimeSpan(0, 13, 14, 41, 410), result);
    }

    [TestMethod]
    public void Inv()
    {
        var converter = new TimeConverterAuto(CultureInfo.InvariantCulture);
        var result = converter.Convert(" 13:14:41.410 ");
        Assert.AreEqual(new TimeSpan(0, 13, 14, 41, 410), result);
    }

    [TestMethod]
    public void InvFallBackToDateTimeParser()
    {
        var converter = new TimeConverterAuto(CultureInfo.InvariantCulture);
        var result = converter.Convert("13: 14:41.410");
        Assert.AreEqual(new TimeSpan(0, 13, 14, 41, 410), result);
    }

    [TestMethod]
    public void InvTimeSpanStringWithDays()
    {
        var converter = new TimeConverterAuto(CultureInfo.InvariantCulture);
        var result = converter.Convert("112:13:14:41.410");
        Assert.AreEqual(new TimeSpan(112, 13, 14, 41, 410), result);
    }

    [TestMethod]
    public void FormattedTimeSpan()
    {
        var converter = new TimeConverterAuto(@"d\.h\:mm", CultureInfo.InvariantCulture);
        var result = converter.Convert("12.7:14");
        Assert.AreEqual(new TimeSpan(12, 7, 14, 0, 0), result);
    }

    [TestMethod]
    public void FormattedTimeSpanFallBack()
    {
        var converter = new TimeConverterAuto(@"d\.hh\:mm", CultureInfo.InvariantCulture);
        var result = converter.Convert("12.7:14");
        Assert.AreEqual(new TimeSpan(12, 7, 14, 0, 0), result);
    }

    [TestMethod]
    public void FormattedTimeSpanFallBackButWrongResult()
    {
        var converter = new TimeConverterAuto(@"d\:hh\:mm", CultureInfo.InvariantCulture);
        var result = converter.Convert("12:7:14");
        Assert.AreEqual(new TimeSpan(0, 12, 7, 14, 0), result);
    }

    [TestMethod]
    public void FormattedDateTime()
    {
        var converter = new TimeConverterAuto("yyyy.MM.dd HH:mm:ss.ffff", CultureInfo.InvariantCulture);
        var result = converter.Convert("2020.02.02 12:07:14.410");
        Assert.AreEqual(new TimeSpan(0, 12, 7, 14, 410), result);
    }

    [TestMethod]
    public void FormattedDateTimeBrokenButTooSmart()
    {
        var converter = new TimeConverterAuto("yyyy.MM.dd HH:mm.ss.ffff", CultureInfo.InvariantCulture);
        var result = converter.Convert("2020.02.02 12:07:14.410");
        Assert.AreEqual(new TimeSpan(0, 12, 7, 14, 410), result);
    }

    [TestMethod]
    public void FormattedDateTimeFinallyBroken()
    {
        var converter = new TimeConverterAuto("yyyy.MM.dd HH:mm.ss.ffff", CultureInfo.InvariantCulture);
        var result = converter.Convert("2020.02.02 12:07.14.410");
        Assert.AreEqual(null, result);
    }
}
