namespace FizzCode.EtLast.Tests.EPPlus;

[TestClass]
public class EpPlusExcelSheetReaderTests
{
    private static EpPlusExcelSheetListReader GetReader(string path)
    {
        return new EpPlusExcelSheetListReader()
        {
            StreamProvider = new LocalFileStreamProvider()
            {
                Path = path,
            },
            AddRowIndexToColumn = "idx",
        };
    }

    [TestMethod]
    public void MissingFileThrowsFileReadException()
    {
        var context = TestExecuter.GetContext();
        var reader = GetReader(@".\TestData\MissingFile.xlsx");

        var builder = SequenceBuilder.Fluent
            .ReadSheetListFromExcel(reader)
            .ThrowExceptionOnRowError();

        var result = TestExecuter.Execute(context, builder);
        Assert.AreEqual(0, result.MutatedRows.Count);
        Assert.AreEqual(1, result.Process.FlowState.Exceptions.Count);
        Assert.IsTrue(result.Process.FlowState.Exceptions[0] is LocalFileReadException);
    }

    [TestMethod]
    public void ListSheets()
    {
        var context = TestExecuter.GetContext();
        var reader = GetReader(@".\TestData\Test.xlsx");

        var builder = SequenceBuilder.Fluent
            .ReadSheetListFromExcel(reader)
            .ThrowExceptionOnRowError();

        var result = TestExecuter.Execute(context, builder);
        Assert.AreEqual(2, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new() { ["Stream"] = @".\TestData\Test.xlsx", ["Index"] = 0, ["Name"] = "MergeAtIndex0", ["Color"] = System.Drawing.Color.FromArgb(0, 0, 0, 0), ["Visible"] = true, ["idx"] = 0 },
            new() { ["Stream"] = @".\TestData\Test.xlsx", ["Index"] = 1, ["Name"] = "DateBroken", ["Color"] = System.Drawing.Color.FromArgb(0, 0, 0, 0), ["Visible"] = true, ["idx"] = 1 } ]);

        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }
}
