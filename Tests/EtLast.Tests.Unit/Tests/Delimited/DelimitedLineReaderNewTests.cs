namespace FizzCode.EtLast.Tests.Unit.Delimited;

[TestClass]
public class DelimitedLineReaderOldTests
{
    private static DelimitedLineReaderOld GetReader(IEtlContext context, string fileName, bool removeSurroundingDoubleQuotes = true)
    {
        return new DelimitedLineReaderOld(context)
        {
            StreamProvider = new LocalFileStreamProvider()
            {
                FileName = fileName,
            },
            Columns = new()
            {
                ["Id"] = new ReaderColumn(new IntConverter()),
                ["Name"] = new ReaderColumn(new StringConverter()),
                ["ValueString"] = new ReaderColumn(new StringConverter()).FromSource("Value1"),
                ["ValueInt"] = new ReaderColumn(new IntConverter()).FromSource("Value2"),
                ["ValueDate"] = new ReaderColumn(new DateConverter()).FromSource("Value3"),
                ["ValueDouble"] = new ReaderColumn(new DoubleConverter()).FromSource("Value4"),
            },
            Header = DelimitedLineHeader.HasHeader,
            RemoveSurroundingDoubleQuotes = removeSurroundingDoubleQuotes
        };
    }

    private static DelimitedLineReaderOld GetSimpleReader(IEtlContext context, string fileName, bool treatEmptyStringsAsNull = true)
    {
        return new DelimitedLineReaderOld(context)
        {
            StreamProvider = new LocalFileStreamProvider()
            {
                FileName = fileName,
            },
            Columns = new()
            {
                ["Id"] = new ReaderColumn(new IntConverter()),
                ["Name"] = new ReaderColumn(new StringConverter()),
                ["Value"] = new ReaderColumn(new StringConverter())
            },
            Header = DelimitedLineHeader.HasHeader,
            TreatEmptyStringAsNull = treatEmptyStringsAsNull,
        };
    }

    [TestMethod]
    public void BasicTest()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
            .ReadDelimitedLinesOld(GetReader(context, @"TestData\Sample.csv"))
            .ReplaceErrorWithValue(new ReplaceErrorWithValueMutator(context)
            {
                Columns = new[] { "ValueDate" },
                Value = null,
            });

        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(2, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 0, ["Name"] = "A", ["ValueString"] = "AAA", ["ValueInt"] = -1 },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 1, ["Name"] = "B", ["ValueInt"] = 3, ["ValueDate"] = new DateTime(2019, 4, 25, 0, 0, 0, 0), ["ValueDouble"] = 1.234d } });

        Assert.AreEqual(0, result.Process.Pipe.Exceptions.Count);
    }

    [TestMethod]
    public void QuotedTest1()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
            .ReadDelimitedLinesOld(GetReader(context, @"TestData\QuotedSample1.csv"));

        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(2, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 1, ["Name"] = "A", ["ValueString"] = "te\"s\"t;test", ["ValueInt"] = -1 },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 2, ["Name"] = "tes\"t;t\"est", ["ValueInt"] = -1 } });

        Assert.AreEqual(0, result.Process.Pipe.Exceptions.Count);
    }

    [TestMethod]
    public void QuotedTest1KeepSurroundingDoubleQuotes()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
            .ReadDelimitedLinesOld(GetReader(context, @"TestData\QuotedSample1.csv", removeSurroundingDoubleQuotes: false))
            .ThrowExceptionOnRowError();

        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(2, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 1, ["Name"] = "A", ["ValueString"] = "\"te\"s\"t;test\"", ["ValueInt"] = -1 },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 2, ["Name"] = "\"tes\"t;t\"est\"", ["ValueInt"] = -1 } });

        Assert.AreEqual(0, result.Process.Pipe.Exceptions.Count);
    }

    [TestMethod]
    public void QuotedTest2()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
            .ReadDelimitedLinesOld(GetSimpleReader(context, @"TestData\QuotedSample2.csv"));

        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(3, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 1, ["Name"] = "A", ["Value"] = "test" },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 2, ["Name"] = "B", ["Value"] = "test\"" },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 3, ["Name"] = "C", ["Value"] = "test\"\"" } });

        Assert.AreEqual(0, result.Process.Pipe.Exceptions.Count);
    }

    [TestMethod]
    public void QuotedTest3EmptyStringsUntouched()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
            .ReadDelimitedLinesOld(GetSimpleReader(context, @"TestData\QuotedSample3.csv", treatEmptyStringsAsNull: false));

        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(8, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 1, ["Name"] = "", ["Value"] = "A" },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 2, ["Name"] = "B", ["Value"] = "" },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 3, ["Name"] = "C", ["Value"] = "\"" },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 4, ["Name"] = "\"", ["Value"] = "D" },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 5, ["Name"] = "E", ["Value"] = "\"\"" },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 6, ["Name"] = "\"\"", ["Value"] = "F" },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 7, ["Name"] = "G", ["Value"] = "\"a\"" },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 8, ["Name"] = "\"b\"", ["Value"] = "H" } });

        Assert.AreEqual(0, result.Process.Pipe.Exceptions.Count);
    }

    [TestMethod]
    public void QuotedTest3EmptyStringsRemoved()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
            .ReadDelimitedLinesOld(GetSimpleReader(context, @"TestData\QuotedSample3.csv", treatEmptyStringsAsNull: true));

        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(8, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 1, ["Value"] = "A" },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 2, ["Name"] = "B" },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 3, ["Name"] = "C", ["Value"] = "\"" },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 4, ["Name"] = "\"", ["Value"] = "D" },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 5, ["Name"] = "E", ["Value"] = "\"\"" },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 6, ["Name"] = "\"\"", ["Value"] = "F" },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 7, ["Name"] = "G", ["Value"] = "\"a\"" },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 8, ["Name"] = "\"b\"", ["Value"] = "H" } });

        Assert.AreEqual(0, result.Process.Pipe.Exceptions.Count);
    }

    [TestMethod]
    public void NewLineTest1()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
            .ReadDelimitedLinesOld(GetReader(context, @"TestData\NewLineSample1.csv"))
            .ReplaceErrorWithValue(new ReplaceErrorWithValueMutator(context)
            {
                Columns = new[] { "ValueDate" },
                Value = null,
            });

        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(1, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 1, ["Name"] = " A", ["ValueString"] = "test\r\n continues", ["ValueInt"] = -1 } });

        Assert.AreEqual(0, result.Process.Pipe.Exceptions.Count);
    }

    [TestMethod]
    public void NewLineTest2()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
            .ReadDelimitedLinesOld(GetReader(context, @"TestData\NewLineSample2.csv"))
            .ReplaceErrorWithValue(new ReplaceErrorWithValueMutator(context)
            {
                Columns = new[] { "ValueDate" },
                Value = null,
            });

        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(1, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 1, ["Name"] = "A", ["ValueString"] = "test\"\r\ncontinues", ["ValueInt"] = -1 } });

        Assert.AreEqual(0, result.Process.Pipe.Exceptions.Count);
    }

    [TestMethod]
    public void InvalidConversion()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
            .ReadDelimitedLinesOld(GetReader(context, @"TestData\SampleInvalidConversion.csv"))
            .ReplaceErrorWithValue(new ReplaceErrorWithValueMutator(context)
            {
                Columns = new[] { "ValueDate" },
                Value = null,
            });

        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(2, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = new EtlRowError("X"), ["Name"] = "A", ["ValueString"] = "AAA", ["ValueInt"] = -1 },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 1, ["Name"] = "B", ["ValueInt"] = 3, ["ValueDate"] = new DateTime(2019, 4, 25, 0, 0, 0, 0), ["ValueDouble"] = 1.234d } });

        Assert.AreEqual(0, result.Process.Pipe.Exceptions.Count);
    }

    [TestMethod]
    public void BrokenHeaderNegative()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
            .ReadDelimitedLinesOld(new DelimitedLineReaderOld(context)
            {
                StreamProvider = new LocalFileStreamProvider()
                {
                    FileName = @"TestData\BrokenHeaderSample.csv",
                },
                Columns = new()
                {
                    ["Id"] = new ReaderColumn(new IntConverter()),
                    ["Name"] = new ReaderColumn(new StringConverter()),
                    ["Value1"] = new ReaderColumn(new StringConverter()),
                    ["Value2"] = new ReaderColumn(new IntConverter()),
                    ["Value3"] = new ReaderColumn(new StringConverter()),
                    ["Value4"] = new ReaderColumn(new StringConverter()),
                },
                Header = DelimitedLineHeader.HasHeader,
            });

        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(2, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 0, ["Name"] = "A", ["Value1"] = "AAA", ["Value2"] = -1 },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 1, ["Name"] = "B", ["Value2"] = 3 } });

        Assert.AreEqual(0, result.Process.Pipe.Exceptions.Count);
    }

    [TestMethod]
    public void BrokenHeaderPositive()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
            .ReadDelimitedLinesOld(new DelimitedLineReaderOld(context)
            {
                StreamProvider = new LocalFileStreamProvider()
                {
                    FileName = @"TestData\BrokenHeaderSample.csv",
                },
                ColumnNames = new[] { "Id", "Name", "Value1", "Value2", "Value3", "Value4" },
                Columns = new()
                {
                    ["Id"] = new ReaderColumn(new IntConverter()),
                    ["Name"] = new ReaderColumn(new StringConverter()),
                    ["Value1"] = new ReaderColumn(new StringConverter()),
                    ["Value2"] = new ReaderColumn(new IntConverter()),
                    ["Value3"] = new ReaderColumn(new StringConverter()),
                    ["Value4"] = new ReaderColumn(new StringConverter()),
                },
                Header = DelimitedLineHeader.IgnoreHeader,
            });

        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(2, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 0, ["Name"] = "A", ["Value1"] = "AAA", ["Value2"] = -1 },
            new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 1, ["Name"] = "B", ["Value2"] = 3, ["Value3"] = "1", ["Value4"] = "1.234" } });

        Assert.AreEqual(0, result.Process.Pipe.Exceptions.Count);
    }
}
