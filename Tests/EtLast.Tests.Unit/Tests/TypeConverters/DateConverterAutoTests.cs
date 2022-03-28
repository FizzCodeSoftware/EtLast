namespace FizzCode.EtLast.Tests.Unit.TypeConverters;

[TestClass]
public class DateConverterAutoAutoTests
{
    [TestMethod]
    public void InvDateTimeString()
    {
        var converter = new DateConverterAuto(CultureInfo.InvariantCulture);
        var result = converter.Convert("2020.05.02 13:14:41.410");
        Assert.AreEqual(new DateTime(2020, 5, 2, 0, 0, 0, 0), result);
    }

    [TestMethod]
    public void InvDateTimeStringWithoutYear()
    {
        var converter = new DateConverterAuto(CultureInfo.InvariantCulture);
        var result = converter.Convert("5.2 13:14:41.410");
        Assert.AreEqual(null, result);
    }

    [TestMethod]
    public void InvDateTimeStringWithoutYear2()
    {
        var converter = new DateConverterAuto(CultureInfo.InvariantCulture);
        var result = converter.Convert("5.2");
        Assert.AreEqual(new DateTime(DateTime.Now.Year, 5, 2), result);
    }

    [TestMethod]
    public void InvDateTimeStringOnlyDateWithPeriodEnd()
    {
        var converter = new DateConverterAuto(CultureInfo.InvariantCulture);
        var result = converter.Convert("2020.5.2.");
        Assert.AreEqual(new DateTime(2020, 5, 2), result);
    }

    [TestMethod]
    public void GbDateTimeString1()
    {
        var converter = new DateConverterAuto(new CultureInfo("en-GB"));
        var result = converter.Convert("05.02.2020 13:14:41.410");
        Assert.AreEqual(new DateTime(2020, 2, 5, 0, 0, 0, 0), result);
    }

    [TestMethod]
    public void GbDateTimeString2()
    {
        var converter = new DateConverterAuto(new CultureInfo("en-GB"));
        var result = converter.Convert("2020.02.05 13:14:41.410");
        Assert.AreEqual(new DateTime(2020, 2, 5, 0, 0, 0, 0), result);
    }

    [TestMethod]
    public void UsDateTimeString1()
    {
        var converter = new DateConverterAuto(new CultureInfo("en-US"));
        var result = converter.Convert("02.05.2020 13:14:41.410");
        Assert.AreEqual(new DateTime(2020, 2, 5, 0, 0, 0, 0), result);
    }

    [TestMethod]
    public void UsDateTimeString2()
    {
        var converter = new DateConverterAuto(new CultureInfo("en-US"));
        var result = converter.Convert("2020.02.05 13:14:41.410");
        Assert.AreEqual(new DateTime(2020, 2, 5, 0, 0, 0, 0), result);
    }

    [TestMethod]
    public void HuLongDateTimeString1()
    {
        var converter = new DateConverterAuto(new CultureInfo("hu-HU"));
        var result = converter.Convert("Február 05.2020 13:14:41.410");
        Assert.AreEqual(new DateTime(2020, 2, 5, 0, 0, 0, 0), result);
    }

    [TestMethod]
    public void HuLongDateTimeString2WithPeriod()
    {
        var converter = new DateConverterAuto(new CultureInfo("hu-HU"));
        var result = converter.Convert("2020. Február 05. 13:14:41.410");
        Assert.AreEqual(new DateTime(2020, 2, 5, 0, 0, 0, 0), result);
    }

    [TestMethod]
    public void EnDateTimeStringWithoutYear()
    {
        var converter = new DateConverterAuto(new CultureInfo("en-EN"));
        var result = converter.Convert("5.2 13:14:41.410");
        Assert.AreEqual(null, result);
    }

    [TestMethod]
    public void EnDateTimeStringWithoutYear2()
    {
        var converter = new DateConverterAuto(new CultureInfo("en-EN"));
        var result = converter.Convert("5.2");
        Assert.AreEqual(new DateTime(DateTime.Now.Year, 5, 2), result);
    }

    [TestMethod]
    public void EnDateTimeStringOnlyDateWithPeriodEnd()
    {
        var converter = new DateConverterAuto(new CultureInfo("en-EN"));
        var result = converter.Convert("2020.5.2.");
        Assert.AreEqual(new DateTime(2020, 5, 2), result);
    }

    [TestMethod]
    public void HuDateTimeString()
    {
        var converter = new DateConverterAuto(new CultureInfo("hu-HU"));
        var result = converter.Convert("2020.05.02 13:14:41.410");
        Assert.AreEqual(new DateTime(2020, 5, 2, 0, 0, 0, 0), result);
    }

    [TestMethod]
    public void HuDateTimeStringWithoutYear()
    {
        var converter = new DateConverterAuto(new CultureInfo("hu-HU"));
        var result = converter.Convert("5.2 13:14:41.410");
        Assert.AreEqual(null, result);
    }

    [TestMethod]
    public void HuDateTimeStringWithoutYear2()
    {
        var converter = new DateConverterAuto(new CultureInfo("hu-HU"));
        var result = converter.Convert("5.2");
        Assert.AreEqual(new DateTime(DateTime.Now.Year, 5, 2), result);
    }

    [TestMethod]
    public void HuDateTimeStringOnlyDateWithPeriodEnd()
    {
        var converter = new DateConverterAuto(new CultureInfo("hu-HU"));
        var result = converter.Convert("2020.5.2.");
        Assert.AreEqual(new DateTime(2020, 5, 2), result);
    }
}
