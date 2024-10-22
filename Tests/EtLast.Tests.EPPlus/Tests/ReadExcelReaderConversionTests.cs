﻿namespace FizzCode.EtLast.Tests.EPPlus;

[TestClass]
public class ReadExcelReaderConversionTests
{
    [TestMethod]
    public void WrapIsWorking()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
            .ReadFromExcel(new EpPlusExcelReader()
            {
                StreamProvider = new LocalFileStreamProvider()
                {
                    Path = @".\TestData\Test.xlsx",
                },
                SheetName = "DateBroken",
                Columns = new()
                {
                    ["Id"] = new ReaderColumn().AsInt(),
                    ["Date"] = new ReaderColumn().AsDate(),
                },
            });

        var result = TestExecuter.Execute(context, builder);
        Assert.AreEqual(2, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new(StringComparer.InvariantCultureIgnoreCase) { ["Id"] = 0, ["Date"] = new EtlRowError(0d) },
            new(StringComparer.InvariantCultureIgnoreCase) { ["Id"] = 1, ["Date"] = new DateTime(2019, 4, 25, 0, 0, 0, 0) } ]);

        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }
}
