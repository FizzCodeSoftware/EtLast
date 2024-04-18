using System.Collections.Generic;

namespace FizzCode.EtLast.Tests.EPPlus;

[TestClass]
public class EpPlusExcelReaderTests
{
    private static EpPlusExcelReader GetReader(IEtlContext context, IManyStreamProvider streamProvider, string sheetName = null, int sheetIndex = -1, bool automaticallyTrimAllStringValues = true)
    {
        return new EpPlusExcelReader()
        {
            StreamProvider = streamProvider,
            SheetName = sheetName,
            SheetIndex = sheetIndex,
            Columns = new()
            {
                ["Id"] = new ReaderColumn().AsInt().ValueWhenSourceIsNull(string.Empty),
                ["Name"] = new ReaderColumn().ValueWhenSourceIsNull(string.Empty),
                ["ValueString"] = new ReaderColumn().FromSource("Value1").ValueWhenSourceIsNull(string.Empty),
                ["ValueInt"] = new ReaderColumn().AsInt().FromSource("Value2"),
                ["ValueDate"] = new ReaderColumn().AsDate().FromSource("Value3"),
                ["ValueDouble"] = new ReaderColumn().AsDouble().FromSource("Value4"),
            },
            AutomaticallyTrimAllStringValues = automaticallyTrimAllStringValues,
        };
    }

    [TestMethod]
    public void MissingFileThrowsFileReadException()
    {
        var context = TestExecuter.GetContext();
        var reader = GetReader(context, new LocalFileStreamProvider() { Path = @".\TestData\MissingFile.xlsx" }, sheetName: "anySheet");

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
        var reader = GetReader(context, new LocalFileStreamProvider() { Path = @".\TestData\Test.xlsx" }, sheetName: "MergeAtIndex0");

        var builder = SequenceBuilder.Fluent
            .ReadFromExcel(reader)
            .ThrowExceptionOnRowError();

        var result = TestExecuter.Execute(context, builder);
        Assert.AreEqual(4, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new(StringComparer.InvariantCultureIgnoreCase) { ["Id"] = 0, ["Name"] = "A", ["ValueString"] = "AAA", ["ValueInt"] = -1, ["ValueDate"] = null, ["ValueDouble"] = null },
            new(StringComparer.InvariantCultureIgnoreCase) { ["Id"] = 1, ["Name"] = "B", ["ValueString"] = "AAA", ["ValueInt"] = 3, ["ValueDate"] = new DateTime(2019, 4, 12, 0, 0, 0, 0), ["ValueDouble"] = 1.234d },
            new(StringComparer.InvariantCultureIgnoreCase) { ["Id"] = 2, ["Name"] = "C", ["ValueString"] = "C", ["ValueInt"] = 3, ["ValueDate"] = new DateTime(2019, 4, 25, 0, 0, 0, 0), ["ValueDouble"] = 1.234d },
            new(StringComparer.InvariantCultureIgnoreCase) { ["Id"] = 3, ["Name"] = "X", ["ValueString"] = "X", ["ValueInt"] = 2, ["ValueDate"] = new DateTime(2019, 4, 25, 0, 0, 0, 0), ["ValueDouble"] = 98d } ]);
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void PartitionedContentBySheetIndex()
    {
        var context = TestExecuter.GetContext();
        var reader = new EpPlusExcelReader()
        {
            StreamProvider = new MultipleLocalFilesInDirectoryStreamProvider()
            {
                Directory = @".\TestData\",
                SearchPattern = "Partition*.xlsx"
            },
            SheetIndex = 0,
            Columns = new()
            {
                ["Age"] = new ReaderColumn().AsInt(),
                ["Name"] = new ReaderColumn().AsString(),
            },
        };

        var builder = SequenceBuilder.Fluent
            .ReadFromExcel(reader)
            .ThrowExceptionOnRowError();

        var result = TestExecuter.Execute(context, builder);
        Assert.AreEqual(9, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new(StringComparer.InvariantCultureIgnoreCase) { ["Name"] = "AAA", ["Age"] = 20 },
            new(StringComparer.InvariantCultureIgnoreCase) { ["Name"] = "BBB", ["Age"] = 25 },
            new(StringComparer.InvariantCultureIgnoreCase) { ["Name"] = "CCC", ["Age"] = 10 },
            new(StringComparer.InvariantCultureIgnoreCase) { ["Name"] = "DDD", ["Age"] = 0 },
            new(StringComparer.InvariantCultureIgnoreCase) { ["Name"] = "EEE", ["Age"] = null },
            new(StringComparer.InvariantCultureIgnoreCase) { ["Name"] = "F", ["Age"] = -1 },
            new(StringComparer.InvariantCultureIgnoreCase) { ["Age"] = 9, ["Name"] = "x" },
            new(StringComparer.InvariantCultureIgnoreCase) { ["Age"] = 10, ["Name"] = "y" },
            new(StringComparer.InvariantCultureIgnoreCase) { ["Age"] = 11, ["Name"] = "z" } ]);
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void ContentBySheetIndex()
    {
        var context = TestExecuter.GetContext();
        var reader = GetReader(context, new LocalFileStreamProvider() { Path = @".\TestData\Test.xlsx" }, sheetIndex: 0);

        var builder = SequenceBuilder.Fluent
            .ReadFromExcel(reader)
            .ThrowExceptionOnRowError();

        var result = TestExecuter.Execute(context, builder);
        Assert.AreEqual(4, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new(StringComparer.InvariantCultureIgnoreCase) { ["Id"] = 0, ["Name"] = "A", ["ValueString"] = "AAA", ["ValueInt"] = -1, ["ValueDate"] = null, ["ValueDouble"] = null },
            new(StringComparer.InvariantCultureIgnoreCase) { ["Id"] = 1, ["Name"] = "B", ["ValueString"] = "AAA", ["ValueInt"] = 3, ["ValueDate"] = new DateTime(2019, 4, 12, 0, 0, 0, 0), ["ValueDouble"] = 1.234d },
            new(StringComparer.InvariantCultureIgnoreCase) { ["Id"] = 2, ["Name"] = "C", ["ValueString"] = "C", ["ValueInt"] = 3, ["ValueDate"] = new DateTime(2019, 4, 25, 0, 0, 0, 0), ["ValueDouble"] = 1.234d },
            new(StringComparer.InvariantCultureIgnoreCase) { ["Id"] = 3, ["Name"] = "X", ["ValueString"] = "X", ["ValueInt"] = 2, ["ValueDate"] = new DateTime(2019, 4, 25, 0, 0, 0, 0), ["ValueDouble"] = 98d } ]);
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void NoTrim()
    {
        var context = TestExecuter.GetContext();
        var reader = GetReader(context, new LocalFileStreamProvider() { Path = @".\TestData\Test.xlsx" }, sheetName: "MergeAtIndex0", automaticallyTrimAllStringValues: false);

        var builder = SequenceBuilder.Fluent
            .ReadFromExcel(reader)
            .ThrowExceptionOnRowError();

        var result = TestExecuter.Execute(context, builder);
        Assert.AreEqual(4, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new(StringComparer.InvariantCultureIgnoreCase) { ["Id"] = 0, ["Name"] = "A   ", ["ValueString"] = "AAA", ["ValueInt"] = -1, ["ValueDate"] = null, ["ValueDouble"] = null },
            new(StringComparer.InvariantCultureIgnoreCase) { ["Id"] = 1, ["Name"] = "B", ["ValueString"] = "AAA", ["ValueInt"] = 3, ["ValueDate"] = new DateTime(2019, 4, 12, 0, 0, 0, 0), ["ValueDouble"] = 1.234d },
            new(StringComparer.InvariantCultureIgnoreCase) { ["Id"] = 2, ["Name"] = "C", ["ValueString"] = "C", ["ValueInt"] = 3, ["ValueDate"] = new DateTime(2019, 4, 25, 0, 0, 0, 0), ["ValueDouble"] = 1.234d },
            new(StringComparer.InvariantCultureIgnoreCase) { ["Id"] = 3, ["Name"] = "X", ["ValueString"] = "X", ["ValueInt"] = 2, ["ValueDate"] = new DateTime(2019, 4, 25, 0, 0, 0, 0), ["ValueDouble"] = 98d } ]);
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }
}
