namespace FizzCode.EtLast.Tests.EPPlus;

[TestClass]
public class EpPlusExcelReaderTests
{
    private static EpPlusExcelReader GetReader(IEtlContext context, IStreamProvider streamProvider, string sheetName = null, int sheetIndex = -1, bool automaticallyTrimAllStringValues = true)
    {
        return new EpPlusExcelReader()
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

        var result = TestExecuter.Execute(context, builder);
        Assert.AreEqual(0, result.MutatedRows.Count);
        Assert.AreEqual(1, result.Process.FlowState.Exceptions.Count);
        Assert.IsTrue(result.Process.FlowState.Exceptions[0] is LocalFileReadException);
    }

    [TestMethod]
    public void ContentBySheetName()
    {
        var context = TestExecuter.GetContext();
        var reader = GetReader(context, new LocalFileStreamProvider() { FileName = @".\TestData\Test.xlsx" }, sheetName: "MergeAtIndex0");

        var builder = SequenceBuilder.Fluent
            .ReadFromExcel(reader)
            .ThrowExceptionOnRowError();

        var result = TestExecuter.Execute(context, builder);
        Assert.AreEqual(4, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new() { ["Id"] = 0, ["Name"] = "A", ["ValueString"] = "AAA", ["ValueInt"] = -1, ["ValueDate"] = null, ["ValueDouble"] = null },
            new() { ["Id"] = 1, ["Name"] = "B", ["ValueString"] = "AAA", ["ValueInt"] = 3, ["ValueDate"] = new DateTime(2019, 4, 12, 0, 0, 0, 0), ["ValueDouble"] = 1.234d },
            new() { ["Id"] = 2, ["Name"] = "C", ["ValueString"] = "C", ["ValueInt"] = 3, ["ValueDate"] = new DateTime(2019, 4, 25, 0, 0, 0, 0), ["ValueDouble"] = 1.234d },
            new() { ["Id"] = 3, ["Name"] = "X", ["ValueString"] = "X", ["ValueInt"] = 2, ["ValueDate"] = new DateTime(2019, 4, 25, 0, 0, 0, 0), ["ValueDouble"] = 98d } ]);
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void PartitionedContentBySheetIndex()
    {
        var context = TestExecuter.GetContext();
        var reader = new EpPlusExcelReader()
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

        var result = TestExecuter.Execute(context, builder);
        Assert.AreEqual(9, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new() { ["Name"] = "AAA", ["Age"] = 20 },
            new() { ["Name"] = "BBB", ["Age"] = 25 },
            new() { ["Name"] = "CCC", ["Age"] = 10 },
            new() { ["Name"] = "DDD", ["Age"] = 0 },
            new() { ["Name"] = "EEE", ["Age"] = null },
            new() { ["Name"] = "F", ["Age"] = -1 },
            new() { ["Age"] = 9, ["Name"] = "x" },
            new() { ["Age"] = 10, ["Name"] = "y" },
            new() { ["Age"] = 11, ["Name"] = "z" } ]);
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void ContentBySheetIndex()
    {
        var context = TestExecuter.GetContext();
        var reader = GetReader(context, new LocalFileStreamProvider() { FileName = @".\TestData\Test.xlsx" }, sheetIndex: 0);

        var builder = SequenceBuilder.Fluent
            .ReadFromExcel(reader)
            .ThrowExceptionOnRowError();

        var result = TestExecuter.Execute(context, builder);
        Assert.AreEqual(4, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new() { ["Id"] = 0, ["Name"] = "A", ["ValueString"] = "AAA", ["ValueInt"] = -1, ["ValueDate"] = null, ["ValueDouble"] = null },
            new() { ["Id"] = 1, ["Name"] = "B", ["ValueString"] = "AAA", ["ValueInt"] = 3, ["ValueDate"] = new DateTime(2019, 4, 12, 0, 0, 0, 0), ["ValueDouble"] = 1.234d },
            new() { ["Id"] = 2, ["Name"] = "C", ["ValueString"] = "C", ["ValueInt"] = 3, ["ValueDate"] = new DateTime(2019, 4, 25, 0, 0, 0, 0), ["ValueDouble"] = 1.234d },
            new() { ["Id"] = 3, ["Name"] = "X", ["ValueString"] = "X", ["ValueInt"] = 2, ["ValueDate"] = new DateTime(2019, 4, 25, 0, 0, 0, 0), ["ValueDouble"] = 98d } ]);
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void NoTrim()
    {
        var context = TestExecuter.GetContext();
        var reader = GetReader(context, new LocalFileStreamProvider() { FileName = @".\TestData\Test.xlsx" }, sheetName: "MergeAtIndex0", automaticallyTrimAllStringValues: false);

        var builder = SequenceBuilder.Fluent
            .ReadFromExcel(reader)
            .ThrowExceptionOnRowError();

        var result = TestExecuter.Execute(context, builder);
        Assert.AreEqual(4, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new() { ["Id"] = 0, ["Name"] = "A   ", ["ValueString"] = "AAA", ["ValueInt"] = -1, ["ValueDate"] = null, ["ValueDouble"] = null },
            new() { ["Id"] = 1, ["Name"] = "B", ["ValueString"] = "AAA", ["ValueInt"] = 3, ["ValueDate"] = new DateTime(2019, 4, 12, 0, 0, 0, 0), ["ValueDouble"] = 1.234d },
            new() { ["Id"] = 2, ["Name"] = "C", ["ValueString"] = "C", ["ValueInt"] = 3, ["ValueDate"] = new DateTime(2019, 4, 25, 0, 0, 0, 0), ["ValueDouble"] = 1.234d },
            new() { ["Id"] = 3, ["Name"] = "X", ["ValueString"] = "X", ["ValueInt"] = 2, ["ValueDate"] = new DateTime(2019, 4, 25, 0, 0, 0, 0), ["ValueDouble"] = 98d } ]);
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }
}
