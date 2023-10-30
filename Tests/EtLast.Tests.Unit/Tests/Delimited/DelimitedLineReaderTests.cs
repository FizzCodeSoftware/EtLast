namespace FizzCode.EtLast.Tests.Unit.Delimited;

[TestClass]
public class DelimitedLineReaderTests
{
    private static DelimitedLineReader GetReader(IEtlContext context, string fileName, bool removeSurroundingDoubleQuotes = true)
    {
        return new DelimitedLineReader(context)
        {
            StreamProvider = new LocalFileStreamProvider()
            {
                FileName = fileName,
            },
            Columns = new()
            {
                ["Id"] = new TextReaderColumn(new IntConverter()),
                ["Name"] = new TextReaderColumn(),
                ["ValueString"] = new TextReaderColumn().FromSource("Value1"),
                ["ValueInt"] = new TextReaderColumn(new IntConverter()).FromSource("Value2"),
                ["ValueDate"] = new TextReaderColumn(new DateConverter()).FromSource("Value3"),
                ["ValueDouble"] = new TextReaderColumn(new DoubleConverter()).FromSource("Value4"),
            },
            Header = DelimitedLineHeader.HasHeader,
            Delimiter = ';',
            RemoveSurroundingDoubleQuotes = removeSurroundingDoubleQuotes
        };
    }

    private static DelimitedLineReader GetSimpleReader(IEtlContext context, string fileName)
    {
        return new DelimitedLineReader(context)
        {
            StreamProvider = new LocalFileStreamProvider()
            {
                FileName = fileName,
            },
            Columns = new()
            {
                ["Id"] = new TextReaderColumn(new IntConverter()),
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
            .ReadDelimitedLines(GetReader(context, @"TestData\Sample.csv"))
            .ReplaceErrorWithValue(new ReplaceErrorWithValueMutator(context)
            {
                Columns = new[] { "ValueDate" },
                Value = null,
            });

        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(2, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new() { ["Id"] = 0, ["Name"] = "A", ["ValueString"] = "AAA", ["ValueInt"] = -1, ["ValueDate"] = null, ["ValueDouble"] = null },
            new() { ["Id"] = 1, ["Name"] = "B", ["ValueString"] = null, ["ValueInt"] = 3, ["ValueDate"] = new DateTime(2019, 4, 25, 0, 0, 0, 0), ["ValueDouble"] = 1.234d } });
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void SkipColumnsTest()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
            .ReadDelimitedLines(new DelimitedLineReader(context)
            {
                StreamProvider = new LocalFileStreamProvider()
                {
                    FileName = @"TestData\Sample.csv",
                },
                Columns = new()
                {
                    ["Id"] = new TextReaderColumn(new IntConverter()),
                    ["Name"] = new TextReaderColumn(),
                    ["ValueDouble"] = new TextReaderColumn(new DoubleConverter()).FromSource("Value4"),
                },
                Header = DelimitedLineHeader.HasHeader,
                Delimiter = ';',
            })
            .ReplaceErrorWithValue(new ReplaceErrorWithValueMutator(context)
            {
                Columns = new[] { "ValueDate" },
                Value = null,
            });

        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(2, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new() { ["Id"] = 0, ["Name"] = "A", ["Value3"] = null, ["ValueDouble"] = null },
            new() { ["Id"] = 1, ["Name"] = "B", ["Value1"] = null, ["ValueDouble"] = 1.234d } });
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void MixedColumnsTest()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
            .ReadDelimitedLines(new DelimitedLineReader(context)
            {
                StreamProvider = new LocalFileStreamProvider()
                {
                    FileName = @"TestData\Sample.csv",
                },
                Columns = new()
                {
                    ["Id"] = new TextReaderColumn(new IntConverter()),
                },
                DefaultColumns = new TextReaderDefaultColumn(),
                Header = DelimitedLineHeader.HasHeader,
                Delimiter = ';',
            });

        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(2, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new() { ["Id"] = 0, ["Name"] = "A", ["Value1"] = "AAA", ["Value2"] = "-1", ["Value3"] = null, ["Value4"] = null },
            new() { ["Id"] = 1, ["Name"] = "B", ["Value1"] = null, ["Value2"] = "3", ["Value3"] = "2019.04.25", ["Value4"] = "1.234" } });
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void QuotedTest1()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
            .ReadDelimitedLines(GetReader(context, @"TestData\QuotedSample1.csv"));

        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(2, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new() { ["Id"] = 1, ["Name"] = "A", ["ValueString"] = "te\"s\"t;test", ["ValueInt"] = -1, ["ValueDate"] = null, ["ValueDouble"] = null },
            new() { ["Id"] = 2, ["Name"] = "tes\"t;t\"est", ["ValueString"] = null, ["ValueInt"] = -1, ["ValueDate"] = null, ["ValueDouble"] = null } });
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void QuotedTest1KeepSurroundingDoubleQuotes()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
            .ReadDelimitedLines(GetReader(context, @"TestData\QuotedSample1.csv", removeSurroundingDoubleQuotes: false))
            .ThrowExceptionOnRowError();

        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(2, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new() { ["Id"] = 1, ["Name"] = "A", ["ValueString"] = "\"te\"s\"t;test\"", ["ValueInt"] = -1, ["ValueDate"] = null, ["ValueDouble"] = null },
            new() { ["Id"] = 2, ["Name"] = "\"tes\"t;t\"est\"", ["ValueString"] = null, ["ValueInt"] = -1, ["ValueDate"] = null, ["ValueDouble"] = null } });
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void QuotedTest2()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
            .ReadDelimitedLines(GetSimpleReader(context, @"TestData\QuotedSample2.csv"));

        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(3, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new() { ["Id"] = 1, ["Name"] = "A", ["Value"] = "test" },
            new() { ["Id"] = 2, ["Name"] = "B", ["Value"] = "test\"" },
            new() { ["Id"] = 3, ["Name"] = "C", ["Value"] = "test\"\"" } });

        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void QuotedTest3()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
            .ReadDelimitedLines(GetSimpleReader(context, @"TestData\QuotedSample3.csv"));

        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(8, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new() { ["Id"] = 1, ["Name"] = null, ["Value"] = "A" },
            new() { ["Id"] = 2, ["Name"] = "B", ["Value"] = null },
            new() { ["Id"] = 3, ["Name"] = "C", ["Value"] = "\"" },
            new() { ["Id"] = 4, ["Name"] = "\"", ["Value"] = "D" },
            new() { ["Id"] = 5, ["Name"] = "E", ["Value"] = "\"\"" },
            new() { ["Id"] = 6, ["Name"] = "\"\"", ["Value"] = "F" },
            new() { ["Id"] = 7, ["Name"] = "G", ["Value"] = "\"a\"" },
            new() { ["Id"] = 8, ["Name"] = "\"b\"", ["Value"] = "H" } });
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void NewLineTest1()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
            .ReadDelimitedLines(GetReader(context, @"TestData\NewLineSample1.csv"))
            .ReplaceErrorWithValue(new ReplaceErrorWithValueMutator(context)
            {
                Columns = new[] { "ValueDate" },
                Value = null,
            });

        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(1, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new() { ["Id"] = 1, ["Name"] = " A", ["ValueString"] = "test\r\n continues", ["ValueInt"] = -1, ["ValueDate"] = null, ["ValueDouble"] = null } });

        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void NewLineTest2()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
            .ReadDelimitedLines(GetReader(context, @"TestData\NewLineSample2.csv"))
            .ReplaceErrorWithValue(new ReplaceErrorWithValueMutator(context)
            {
                Columns = new[] { "ValueDate" },
                Value = null,
            });

        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(1, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new() { ["Id"] = 1, ["Name"] = "A", ["ValueString"] = "test\"\r\ncontinues", ["ValueInt"] = -1, ["ValueDate"] = null, ["ValueDouble"] = null } });

        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void InvalidConversion()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
            .ReadDelimitedLines(GetReader(context, @"TestData\SampleInvalidConversion.csv"))
            .ReplaceErrorWithValue(new ReplaceErrorWithValueMutator(context)
            {
                Columns = new[] { "ValueDate" },
                Value = null,
            });

        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(2, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new() { ["Id"] = new EtlRowError("X"), ["Name"] = "A", ["ValueString"] = "AAA", ["ValueInt"] = -1, ["ValueDate"] = null, ["ValueDouble"] = null },
            new() { ["Id"] = 1, ["Name"] = "B", ["ValueString"] = null, ["ValueInt"] = 3, ["ValueDate"] = new DateTime(2019, 4, 25, 0, 0, 0, 0), ["ValueDouble"] = 1.234d } });
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void BrokenHeaderNegative()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
            .ReadDelimitedLines(new DelimitedLineReader(context)
            {
                StreamProvider = new LocalFileStreamProvider()
                {
                    FileName = @"TestData\BrokenHeaderSample.csv",
                },
                Columns = new()
                {
                    ["Id"] = new TextReaderColumn(new IntConverter()),
                    ["Name"] = new TextReaderColumn(),
                    ["Value1"] = new TextReaderColumn(),
                    ["Value2"] = new TextReaderColumn(new IntConverter()),
                    ["Value3"] = new TextReaderColumn(),
                    ["Value4"] = new TextReaderColumn(),
                },
                Header = DelimitedLineHeader.HasHeader,
                Delimiter = ';',
            });

        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(2, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new() { ["Id"] = 0, ["Name"] = "A", ["Value1"] = "AAA", ["Value2"] = -1, ["sg"] = null },
            new() { ["Id"] = 1, ["Name"] = "B", ["Value1"] = null, ["Value2"] = 3 } });
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void BrokenHeaderPositive()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
            .ReadDelimitedLines(new DelimitedLineReader(context)
            {
                StreamProvider = new LocalFileStreamProvider()
                {
                    FileName = @"TestData\BrokenHeaderSample.csv",
                },
                ColumnNames = new[] { "Id", "Name", "Value1", "Value2", "Value3", "Value4" },
                Columns = new()
                {
                    ["Id"] = new TextReaderColumn(new IntConverter()),
                    ["Name"] = new TextReaderColumn(),
                    ["Value1"] = new TextReaderColumn(),
                    ["Value2"] = new TextReaderColumn(new IntConverter()),
                    ["Value3"] = new TextReaderColumn(),
                    ["Value4"] = new TextReaderColumn(),
                },
                Header = DelimitedLineHeader.IgnoreHeader,
                Delimiter = ';',
            });

        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(2, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new() { ["Id"] = 0, ["Name"] = "A", ["Value1"] = "AAA", ["Value2"] = -1, ["Value3"] = null, ["Value4"] = null },
            new() { ["Id"] = 1, ["Name"] = "B", ["Value1"] = null, ["Value2"] = 3, ["Value3"] = "1", ["Value4"] = "1.234" } });
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }
}
