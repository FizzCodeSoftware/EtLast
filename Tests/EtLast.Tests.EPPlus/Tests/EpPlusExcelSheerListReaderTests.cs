namespace FizzCode.EtLast.Tests.EPPlus;

[TestClass]
public class EpPlusExcelSheetReaderTests
{
    private static EpPlusExcelSheetListReader GetReader(IEtlContext context, string fileName)
    {
        return new EpPlusExcelSheetListReader(context)
        {
            StreamProvider = new LocalFileStreamProvider()
            {
                FileName = fileName,
            },
            AddRowIndexToColumn = "idx",
        };
    }

    [TestMethod]
    public void MissingFileThrowsFileReadException()
    {
        var context = TestExecuter.GetContext();
        var reader = GetReader(context, @".\TestData\MissingFile.xlsx");

        var builder = SequenceBuilder.Fluent
            .ReadFrom(reader)
            .ThrowExceptionOnRowError();

        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(0, result.MutatedRows.Count);
        Assert.AreEqual(1, result.Process.InvocationContext.Exceptions.Count);
        Assert.IsTrue(result.Process.InvocationContext.Exceptions[0] is LocalFileReadException);
    }

    [TestMethod]
    public void ListSheets()
    {
        var context = TestExecuter.GetContext();
        var reader = GetReader(context, @".\TestData\Test.xlsx");

        var builder = SequenceBuilder.Fluent
            .ReadFrom(reader)
            .ThrowExceptionOnRowError();

        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(2, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new CaseInsensitiveStringKeyDictionary<object>() { ["Stream"] = @".\TestData\Test.xlsx", ["Index"] = 0, ["Name"] = "MergeAtIndex0", ["Color"] = System.Drawing.Color.FromArgb(0, 0, 0, 0), ["Visible"] = true, ["idx"] = 0 },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Stream"] = @".\TestData\Test.xlsx", ["Index"] = 1, ["Name"] = "DateBroken", ["Color"] = System.Drawing.Color.FromArgb(0, 0, 0, 0), ["Visible"] = true, ["idx"] = 1 } });

        Assert.AreEqual(0, result.Process.InvocationContext.Exceptions.Count);
    }
}
