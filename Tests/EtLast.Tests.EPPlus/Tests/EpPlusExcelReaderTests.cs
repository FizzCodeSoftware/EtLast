namespace FizzCode.EtLast.Tests.EPPlus;

[TestClass]
public class EpPlusExcelReaderTests
{
    private static EpPlusExcelReader GetReader(IEtlContext context, IStreamProvider streamProvider, string sheetName = null, int sheetIndex = -1, bool automaticallyTrimAllStringValues = true)
    {
        return new EpPlusExcelReader(context)
        {
            StreamProvider = streamProvider,
            SheetName = sheetName,
            SheetIndex = sheetIndex,
            Columns = new()
            {
                ["Id"] = new ReaderColumn(new IntConverter()).ValueWhenSourceIsNull(string.Empty),
                ["Name"] = new ReaderColumn().ValueWhenSourceIsNull(string.Empty),
                ["ValueString"] = new ReaderColumn().FromSource("Value1").ValueWhenSourceIsNull(string.Empty),
                ["ValueInt"] = new ReaderColumn(new IntConverter()).FromSource("Value2"),
                ["ValueDate"] = new ReaderColumn(new DateConverter()).FromSource("Value3"),
                ["ValueDouble"] = new ReaderColumn(new DoubleConverter()).FromSource("Value4"),
            },
            AutomaticallyTrimAllStringValues = automaticallyTrimAllStringValues,
        };
    }

    [TestMethod]
    public void MissingFileThrowsFileReadException()
    {
        var context = TestExecuter.GetContext();
        var reader = GetReader(context, new LocalFileStreamProvider() { FileName = @".\TestData\MissingFile.xlsx" }, sheetName: "anySheet");

        var builder = SequenceBuilder.Fluent
            .ReadFromExcel(reader)
            .ThrowExceptionOnRowError();

        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(0, result.MutatedRows.Count);
        Assert.AreEqual(1, result.Process.Pipe.Exceptions.Count);
        Assert.IsTrue(result.Process.Pipe.Exceptions[0] is LocalFileReadException);
    }

    [TestMethod]
    public void ContentBySheetName()
    {
        var context = TestExecuter.GetContext();
        var reader = GetReader(context, new LocalFileStreamProvider() { FileName = @".\TestData\Test.xlsx" }, sheetName: "MergeAtIndex0");

        var builder = SequenceBuilder.Fluent
            .ReadFromExcel(reader)
            .ThrowExceptionOnRowError();

        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(4, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 0, ["Name"] = "A", ["ValueString"] = "AAA", ["ValueInt"] = -1},
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 1, ["Name"] = "B", ["ValueString"] = "AAA", ["ValueInt"] = 3, ["ValueDate"] = new DateTime(2019, 4, 12, 0, 0, 0, 0), ["ValueDouble"] = 1.234d },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 2, ["Name"] = "C", ["ValueString"] = "C", ["ValueInt"] = 3, ["ValueDate"] = new DateTime(2019, 4, 25, 0, 0, 0, 0), ["ValueDouble"] = 1.234d },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 3, ["Name"] = "X", ["ValueString"] = "X", ["ValueInt"] = 2, ["ValueDate"] = new DateTime(2019, 4, 25, 0, 0, 0, 0), ["ValueDouble"] = 98d } });

        Assert.AreEqual(0, result.Process.Pipe.Exceptions.Count);
    }

    [TestMethod]
    public void PartitionedContentBySheetIndex()
    {
        var context = TestExecuter.GetContext();
        var reader = new EpPlusExcelReader(context)
        {
            StreamProvider = new LocalDirectoryStreamProvider()
            {
                Directory = @".\TestData\",
                SearchPattern = "Partition*.xlsx"
            },
            SheetIndex = 0,
            Columns = new()
            {
                ["Age"] = new ReaderColumn(new IntConverter()),
                ["Name"] = new ReaderColumn(new StringConverter()),
            },
        };

        var builder = SequenceBuilder.Fluent
            .ReadFromExcel(reader)
            .ThrowExceptionOnRowError();

        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(9, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new CaseInsensitiveStringKeyDictionary<object>() { ["Name"] = "AAA", ["Age"] = 20 },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Name"] = "BBB", ["Age"] = 25 },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Name"] = "CCC", ["Age"] = 10 },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Name"] = "DDD", ["Age"] = 0 },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Name"] = "EEE" },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Name"] = "F", ["Age"] = -1 },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Age"] = 9, ["Name"] = "x" },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Age"] = 10, ["Name"] = "y" },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Age"] = 11, ["Name"] = "z" } });

        Assert.AreEqual(0, result.Process.Pipe.Exceptions.Count);
    }

    [TestMethod]
    public void ContentBySheetIndex()
    {
        var context = TestExecuter.GetContext();
        var reader = GetReader(context, new LocalFileStreamProvider() { FileName = @".\TestData\Test.xlsx" }, sheetIndex: 0);

        var builder = SequenceBuilder.Fluent
            .ReadFromExcel(reader)
            .ThrowExceptionOnRowError();

        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(4, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 0, ["Name"] = "A", ["ValueString"] = "AAA", ["ValueInt"] = -1 },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 1, ["Name"] = "B", ["ValueString"] = "AAA", ["ValueInt"] = 3, ["ValueDate"] = new DateTime(2019, 4, 12, 0, 0, 0, 0), ["ValueDouble"] = 1.234d },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 2, ["Name"] = "C", ["ValueString"] = "C", ["ValueInt"] = 3, ["ValueDate"] = new DateTime(2019, 4, 25, 0, 0, 0, 0), ["ValueDouble"] = 1.234d },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 3, ["Name"] = "X", ["ValueString"] = "X", ["ValueInt"] = 2, ["ValueDate"] = new DateTime(2019, 4, 25, 0, 0, 0, 0), ["ValueDouble"] = 98d } });

        Assert.AreEqual(0, result.Process.Pipe.Exceptions.Count);
    }

    [TestMethod]
    public void NoTrim()
    {
        var context = TestExecuter.GetContext();
        var reader = GetReader(context, new LocalFileStreamProvider() { FileName = @".\TestData\Test.xlsx" }, sheetName: "MergeAtIndex0", automaticallyTrimAllStringValues: false);

        var builder = SequenceBuilder.Fluent
            .ReadFromExcel(reader)
            .ThrowExceptionOnRowError();

        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(4, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 0, ["Name"] = "A   ", ["ValueString"] = "AAA", ["ValueInt"] = -1,},
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 1, ["Name"] = "B", ["ValueString"] = "AAA", ["ValueInt"] = 3, ["ValueDate"] = new DateTime(2019, 4, 12, 0, 0, 0, 0), ["ValueDouble"] = 1.234d },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 2, ["Name"] = "C", ["ValueString"] = "C", ["ValueInt"] = 3, ["ValueDate"] = new DateTime(2019, 4, 25, 0, 0, 0, 0), ["ValueDouble"] = 1.234d },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 3, ["Name"] = "X", ["ValueString"] = "X", ["ValueInt"] = 2, ["ValueDate"] = new DateTime(2019, 4, 25, 0, 0, 0, 0), ["ValueDouble"] = 98d } });

        Assert.AreEqual(0, result.Process.Pipe.Exceptions.Count);
    }
}
