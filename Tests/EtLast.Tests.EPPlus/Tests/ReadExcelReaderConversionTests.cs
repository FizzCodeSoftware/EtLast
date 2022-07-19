namespace FizzCode.EtLast.Tests.EPPlus;

[TestClass]
public class ReadExcelReaderConversionTests
{
    [TestMethod]
    public void WrapIsWorking()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
            .ReadFromExcel(new EpPlusExcelReader(context)
            {
                StreamProvider = new LocalFileStreamProvider()
                {
                    FileName = @".\TestData\Test.xlsx",
                },
                SheetName = "DateBroken",
                Columns = new()
                {
                    ["Id"] = new ReaderColumn(new IntConverter()),
                    ["Date"] = new ReaderColumn(new DateConverter()),
                },
            });

        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(2, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 0, ["Date"] = new EtlRowError(0d) },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 1, ["Date"] = new DateTime(2019, 4, 25, 0, 0, 0, 0) } });
        var exceptions = context.GetExceptions();
        Assert.AreEqual(0, exceptions.Count);
    }
}
