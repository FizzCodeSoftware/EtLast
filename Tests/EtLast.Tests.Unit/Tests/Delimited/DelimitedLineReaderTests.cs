namespace FizzCode.EtLast.Tests.Unit.Delimited;

[TestClass]
public class DelimitedLineReaderTests
{
    private static DelimitedLineReader GetReader(string path, bool removeSurroundingDoubleQuotes = true)
    {
        return new DelimitedLineReader()
        {
            StreamProvider = new LocalFileStreamProvider()
            {
                Path = path,
            },
            Columns = new()
            {
                ["Id"] = new TextReaderColumn().AsInt(),
                ["Name"] = new TextReaderColumn(),
                ["ValueString"] = new TextReaderColumn().FromSource("Value1"),
                ["ValueInt"] = new TextReaderColumn().AsInt().FromSource("Value2"),
                ["ValueDate"] = new TextReaderColumn().AsDate().FromSource("Value3"),
                ["ValueDouble"] = new TextReaderColumn().AsDouble().FromSource("Value4"),
            },
            Header = DelimitedLineHeader.HasHeader,
            Delimiter = ';',
            RemoveSurroundingDoubleQuotes = removeSurroundingDoubleQuotes
        };
    }

    private static DelimitedLineReader GetSimpleReader(string path)
    {
        return new DelimitedLineReader()
        {
            StreamProvider = new LocalFileStreamProvider()
            {
                Path = path,
            },
            Columns = new()
            {
                ["Id"] = new TextReaderColumn().AsInt(),
                ["Name"] = new TextReaderColumn(),
                ["Value"] = new TextReaderColumn()
            },
            Header = DelimitedLineHeader.HasHeader,
            Delimiter = ';',
        };
    }

    [TestMethod]
    public void BasicTest()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
            .ReadDelimitedLines(GetReader(@"TestData\Sample.csv"))
            .ReplaceErrorWithValue(new ReplaceErrorWithValueMutator()
            {
                Columns = ["ValueDate"],
                Value = null,
            });

        var result = TestExecuter.Execute(context, builder);
        Assert.AreEqual(2, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new() { ["Id"] = 0, ["Name"] = "A", ["ValueString"] = "AAA", ["ValueInt"] = -1, ["ValueDate"] = null, ["ValueDouble"] = null },
            new() { ["Id"] = 1, ["Name"] = "B", ["ValueString"] = null, ["ValueInt"] = 3, ["ValueDate"] = new DateTime(2019, 4, 25, 0, 0, 0, 0), ["ValueDouble"] = 1.234d }]);
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void SkipColumnsTest()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
            .ReadDelimitedLines(new DelimitedLineReader()
            {
                StreamProvider = new LocalFileStreamProvider()
                {
                    Path = @"TestData\Sample.csv",
                },
                Columns = new()
                {
                    ["Id"] = new TextReaderColumn().AsInt(),
                    ["Name"] = new TextReaderColumn(),
                    ["ValueDouble"] = new TextReaderColumn().AsDouble().FromSource("Value4"),
                },
                Header = DelimitedLineHeader.HasHeader,
                Delimiter = ';',
            })
            .ReplaceErrorWithValue(new ReplaceErrorWithValueMutator()
            {
                Columns = ["ValueDate"],
                Value = null,
            });

        var result = TestExecuter.Execute(context, builder);
        Assert.AreEqual(2, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new() { ["Id"] = 0, ["Name"] = "A", ["Value3"] = null, ["ValueDouble"] = null },
            new() { ["Id"] = 1, ["Name"] = "B", ["Value1"] = null, ["ValueDouble"] = 1.234d }]);
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void MixedColumnsTest()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
            .ReadDelimitedLines(new DelimitedLineReader()
            {
                StreamProvider = new LocalFileStreamProvider()
                {
                    Path = @"TestData\Sample.csv",
                },
                Columns = new()
                {
                    ["Id"] = new TextReaderColumn().AsInt(),
                },
                DefaultColumns = new TextReaderColumn(),
                Header = DelimitedLineHeader.HasHeader,
                Delimiter = ';',
            });

        var result = TestExecuter.Execute(context, builder);
        Assert.AreEqual(2, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new() { ["Id"] = 0, ["Name"] = "A", ["Value1"] = "AAA", ["Value2"] = "-1", ["Value3"] = null, ["Value4"] = null },
            new() { ["Id"] = 1, ["Name"] = "B", ["Value1"] = null, ["Value2"] = "3", ["Value3"] = "2019.04.25", ["Value4"] = "1.234" }]);
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void QuotedTest1()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
            .ReadDelimitedLines(GetReader(@"TestData\QuotedSample1.csv"));

        var result = TestExecuter.Execute(context, builder);
        Assert.AreEqual(2, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new() { ["Id"] = 1, ["Name"] = "A", ["ValueString"] = "te\"s\"t;test", ["ValueInt"] = -1, ["ValueDate"] = null, ["ValueDouble"] = null },
            new() { ["Id"] = 2, ["Name"] = "tes\"t;t\"est", ["ValueString"] = null, ["ValueInt"] = -1, ["ValueDate"] = null, ["ValueDouble"] = null }]);
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void QuotedTest1KeepSurroundingDoubleQuotes()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
            .ReadDelimitedLines(GetReader(@"TestData\QuotedSample1.csv", removeSurroundingDoubleQuotes: false))
            .ThrowExceptionOnRowError();

        var result = TestExecuter.Execute(context, builder);
        Assert.AreEqual(2, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new() { ["Id"] = 1, ["Name"] = "A", ["ValueString"] = "\"te\"s\"t;test\"", ["ValueInt"] = -1, ["ValueDate"] = null, ["ValueDouble"] = null },
            new() { ["Id"] = 2, ["Name"] = "\"tes\"t;t\"est\"", ["ValueString"] = null, ["ValueInt"] = -1, ["ValueDate"] = null, ["ValueDouble"] = null }]);
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void QuotedTest2()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
            .ReadDelimitedLines(GetSimpleReader(@"TestData\QuotedSample2.csv"));

        var result = TestExecuter.Execute(context, builder);
        Assert.AreEqual(3, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new() { ["Id"] = 1, ["Name"] = "A", ["Value"] = "test" },
            new() { ["Id"] = 2, ["Name"] = "B", ["Value"] = "test\"" },
            new() { ["Id"] = 3, ["Name"] = "C", ["Value"] = "test\"\"" }]);

        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void QuotedTest3()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
            .ReadDelimitedLines(GetSimpleReader(@"TestData\QuotedSample3.csv"));

        var result = TestExecuter.Execute(context, builder);
        Assert.AreEqual(8, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new() { ["Id"] = 1, ["Name"] = null, ["Value"] = "A" },
            new() { ["Id"] = 2, ["Name"] = "B", ["Value"] = null },
            new() { ["Id"] = 3, ["Name"] = "C", ["Value"] = "\"" },
            new() { ["Id"] = 4, ["Name"] = "\"", ["Value"] = "D" },
            new() { ["Id"] = 5, ["Name"] = "E", ["Value"] = "\"\"" },
            new() { ["Id"] = 6, ["Name"] = "\"\"", ["Value"] = "F" },
            new() { ["Id"] = 7, ["Name"] = "G", ["Value"] = "\"a\"" },
            new() { ["Id"] = 8, ["Name"] = "\"b\"", ["Value"] = "H" }]);
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void NewLineTest1()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
            .ReadDelimitedLines(GetReader(@"TestData\NewLineSample1.csv"))
            .ReplaceErrorWithValue(new ReplaceErrorWithValueMutator()
            {
                Columns = ["ValueDate"],
                Value = null,
            });

        var result = TestExecuter.Execute(context, builder);
        Assert.AreEqual(1, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new() { ["Id"] = 1, ["Name"] = " A", ["ValueString"] = "test\r\n continues", ["ValueInt"] = -1, ["ValueDate"] = null, ["ValueDouble"] = null }]);

        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void NewLineTest2()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
            .ReadDelimitedLines(GetReader(@"TestData\NewLineSample2.csv"))
            .ReplaceErrorWithValue(new ReplaceErrorWithValueMutator()
            {
                Columns = ["ValueDate"],
                Value = null,
            });

        var result = TestExecuter.Execute(context, builder);
        Assert.AreEqual(1, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new() { ["Id"] = 1, ["Name"] = "A", ["ValueString"] = "test\"\r\ncontinues", ["ValueInt"] = -1, ["ValueDate"] = null, ["ValueDouble"] = null }]);

        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    /// <summary>
    /// New empty line test
    /// </summary>
    [TestMethod]
    public void NewLineTest3()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
            .ReadDelimitedLines(GetReader(@"TestData\NewLineSample3.csv"))
            .ReplaceErrorWithValue(new ReplaceErrorWithValueMutator()
            {
                Columns = ["ValueDate"],
                Value = null,
            });

        var result = TestExecuter.Execute(context, builder);
        Assert.AreEqual(2, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new() { ["Id"] = 1, ["Name"] = "A", ["ValueString"] = "test\"\r\n\r\ncontinues", ["ValueInt"] = -1, ["ValueDate"] = null, ["ValueDouble"] = null },
            new() { ["Id"] = 2, ["Name"] = "B", ["ValueString"] = "test2", ["ValueInt"] = null, ["ValueDate"] = null, ["ValueDouble"] = null }
        ]);

        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void NewLineTest4()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
            .ReadDelimitedLines(GetReader(@"TestData\NewLineSample4.csv"))
            .ReplaceErrorWithValue(new ReplaceErrorWithValueMutator()
            {
                Columns = ["ValueDate"],
                Value = null,
            });

        var result = TestExecuter.Execute(context, builder);
        Assert.AreEqual(2, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new() { ["Id"] = 1, ["Name"] = "A", ["ValueString"] = "\r\ntest continues", ["ValueInt"] = -1, ["ValueDate"] = null, ["ValueDouble"] = null },
            new() { ["Id"] = 2, ["Name"] = "B", ["ValueString"] = "test2", ["ValueInt"] = null, ["ValueDate"] = null, ["ValueDouble"] = null }
        ]);

        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void InvalidConversion()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
            .ReadDelimitedLines(GetReader(@"TestData\SampleInvalidConversion.csv"))
            .ReplaceErrorWithValue(new ReplaceErrorWithValueMutator()
            {
                Columns = ["ValueDate"],
                Value = null,
            });

        var result = TestExecuter.Execute(context, builder);
        Assert.AreEqual(2, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new() { ["Id"] = new EtlRowError("X"), ["Name"] = "A", ["ValueString"] = "AAA", ["ValueInt"] = -1, ["ValueDate"] = null, ["ValueDouble"] = null },
            new() { ["Id"] = 1, ["Name"] = "B", ["ValueString"] = null, ["ValueInt"] = 3, ["ValueDate"] = new DateTime(2019, 4, 25, 0, 0, 0, 0), ["ValueDouble"] = 1.234d }]);
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void BrokenHeaderNegative()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
            .ReadDelimitedLines(new DelimitedLineReader()
            {
                StreamProvider = new LocalFileStreamProvider()
                {
                    Path = @"TestData\BrokenHeaderSample.csv",
                },
                Columns = new()
                {
                    ["Id"] = new TextReaderColumn().AsInt(),
                    ["Name"] = new TextReaderColumn(),
                    ["Value1"] = new TextReaderColumn(),
                    ["Value2"] = new TextReaderColumn().AsInt(),
                    ["Value3"] = new TextReaderColumn(),
                    ["Value4"] = new TextReaderColumn(),
                },
                Header = DelimitedLineHeader.HasHeader,
                Delimiter = ';',
            });

        var result = TestExecuter.Execute(context, builder);
        Assert.AreEqual(2, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new() { ["Id"] = 0, ["Name"] = "A", ["Value1"] = "AAA", ["Value2"] = -1, ["sg"] = null },
            new() { ["Id"] = 1, ["Name"] = "B", ["Value1"] = null, ["Value2"] = 3 }]);
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void BrokenHeaderPositive()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
            .ReadDelimitedLines(new DelimitedLineReader()
            {
                StreamProvider = new LocalFileStreamProvider()
                {
                    Path = @"TestData\BrokenHeaderSample.csv",
                },
                ColumnNames = ["Id", "Name", "Value1", "Value2", "Value3", "Value4"],
                Columns = new()
                {
                    ["Id"] = new TextReaderColumn().AsInt(),
                    ["Name"] = new TextReaderColumn(),
                    ["Value1"] = new TextReaderColumn(),
                    ["Value2"] = new TextReaderColumn().AsInt(),
                    ["Value3"] = new TextReaderColumn(),
                    ["Value4"] = new TextReaderColumn(),
                },
                Header = DelimitedLineHeader.IgnoreHeader,
                Delimiter = ';',
            });

        var result = TestExecuter.Execute(context, builder);
        Assert.AreEqual(2, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new() { ["Id"] = 0, ["Name"] = "A", ["Value1"] = "AAA", ["Value2"] = -1, ["Value3"] = null, ["Value4"] = null },
            new() { ["Id"] = 1, ["Name"] = "B", ["Value1"] = null, ["Value2"] = 3, ["Value3"] = "1", ["Value4"] = "1.234" }]);
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }
}
