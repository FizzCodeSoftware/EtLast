namespace FizzCode.EtLast.Tests.Unit.Delimited;

[TestClass]
public class WriteToDelimitedMutatorTests
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

    [TestMethod]
    public void TestSinkValidator()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
            .ReadFrom(TestData.Person(context))
            .WriteToDelimited(new WriteToDelimitedMutator(context)
            {
                Columns = [],
                SinkProvider = new LocalFileSinkProvider()
                {
                    FileNameGenerator = null, // should throw an exception
                    ActionWhenFileExists = LocalSinkFileExistsAction.Continue,
                    FileMode = FileMode.Append,
                },
            });

        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(0, result.MutatedRows.Count);
        Assert.AreEqual(1, result.Process.FlowState.Exceptions.Count);
        Assert.IsTrue(result.Process.FlowState.Exceptions[0] is InvalidProcessParameterException);
    }

    [TestMethod]
    public void PersonWriterWithReaderTest()
    {
        using (var outputStream = new MemoryStream())
        {
            var context = TestExecuter.GetContext();
            var builder = SequenceBuilder.Fluent
                .ReadFrom(TestData.Person(context))
                .WriteToDelimited(new WriteToDelimitedMutator(context)
                {
                    Delimiter = ';',
                    WriteHeader = true,
                    Encoding = Encoding.UTF8,
                    FormatProvider = CultureInfo.InvariantCulture,
                    SinkProvider = new MemorySinkProvider()
                    {
                        StreamCreator = () => outputStream,
                        AutomaticallyDispose = false,
                    },
                    Columns = new()
                    {
                        ["id"] = null,
                        ["name"] = null,
                        ["age"] = null,
                        ["HeightInCm"] = new DelimitedColumn().FromSource("height"),
                        ["eyeColor"] = null,
                        ["countryId"] = null,
                        ["birthDate"] = null,
                        ["lastChangedTime"] = null,
                    },
                });

            var result = TestExecuter.Execute(builder);
            outputStream.Position = 0;
            var data = Encoding.UTF8.GetString(outputStream.ToArray());

            Assert.AreEqual(@"id;name;age;HeightInCm;eyeColor;countryId;birthDate;lastChangedTime
0;A;17;160;brown;1;2010.12.09 00:00:00.0000000;2015.12.19 12:00:01.0000000
1;B;8;190;;1;2011.02.01 00:00:00.0000000;2015.12.19 13:02:00.0000000
2;C;27;170;green;2;2014.01.21 00:00:00.0000000;2015.11.21 17:11:58.0000000
3;D;39;160;fake;;2018.07.11;2017.08.01 04:09:01.0000000
4;E;-3;160;;1;;2019.01.01 23:59:59.0000000
5;A;11;140;;;2013.05.15 00:00:00.0000000;2018.01.01 00:00:00.0000000
6;fake;;140;;5;2018.01.09 00:00:00.0000000;
", data);

            outputStream.Position = 0;

            context = TestExecuter.GetContext();
            builder = SequenceBuilder.Fluent
                .ReadDelimitedLines(new DelimitedLineReader(context)
                {
                    StreamProvider = new MemoryStreamProvider()
                    {
                        StreamCreator = () => outputStream,
                    },
                    Columns = new()
                    {
                        ["id"] = new TextReaderColumn(new IntConverter()),
                        ["name"] = new TextReaderColumn(),
                        ["age"] = new TextReaderColumn(new IntConverter()),
                        ["HeightInCm"] = new TextReaderColumn(new IntConverter()),
                        ["eyeColor"] = new TextReaderColumn(),
                        ["countryId"] = new TextReaderColumn(new IntConverter()),
                        ["birthDate"] = new TextReaderColumn(new DateTimeConverter()),
                        ["lastChangedTime"] = new TextReaderColumn(new DateTimeConverter()),
                    },
                    Header = DelimitedLineHeader.HasHeader,
                    Delimiter = ';',
                });

            result = TestExecuter.Execute(builder);
            Assert.AreEqual(7, result.MutatedRows.Count);
            Assert.That.ExactMatch(result.MutatedRows, [
            new() { ["id"] = 0, ["name"] = "A", ["age"] = 17, ["HeightInCm"] = 160, ["eyeColor"] = "brown", ["countryId"] = 1, ["birthDate"] = new DateTime(2010, 12, 9, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 12, 0, 1, 0) },
            new() { ["id"] = 1, ["name"] = "B", ["age"] = 8, ["HeightInCm"] = 190, ["eyeColor"] = null, ["countryId"] = 1, ["birthDate"] = new DateTime(2011, 2, 1, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 13, 2, 0, 0) },
            new() { ["id"] = 2, ["name"] = "C", ["age"] = 27, ["HeightInCm"] = 170, ["eyeColor"] = "green", ["countryId"] = 2, ["birthDate"] = new DateTime(2014, 1, 21, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 11, 21, 17, 11, 58, 0) },
            new() { ["id"] = 3, ["name"] = "D", ["age"] = 39, ["HeightInCm"] = 160, ["eyeColor"] = "fake", ["countryId"] = null, ["birthDate"] = new DateTime(2018, 7, 11, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2017, 8, 1, 4, 9, 1, 0) },
            new() { ["id"] = 4, ["name"] = "E", ["age"] = -3, ["HeightInCm"] = 160, ["eyeColor"] = null, ["countryId"] = 1, ["birthDate"] = null, ["lastChangedTime"] = new DateTime(2019, 1, 1, 23, 59, 59, 0) },
            new() { ["id"] = 5, ["name"] = "A", ["age"] = 11, ["HeightInCm"] = 140, ["eyeColor"] = null, ["countryId"] = null, ["birthDate"] = new DateTime(2013, 5, 15, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2018, 1, 1, 0, 0, 0, 0) },
            new() { ["id"] = 6, ["name"] = "fake", ["age"] = null, ["HeightInCm"] = 140, ["eyeColor"] = null, ["countryId"] = 5, ["birthDate"] = new DateTime(2018, 1, 9, 0, 0, 0, 0) } ]);
            Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
        }
    }

    [TestMethod]
    public void NewLineTest()
    {
        using (var outputStream = new MemoryStream())
        {
            var context = TestExecuter.GetContext();
            var builder = SequenceBuilder.Fluent
                .ReadDelimitedLines(GetReader(context, @"TestData\NewLineSample1.csv"))
                .ReplaceErrorWithValue(new ReplaceErrorWithValueMutator(context)
                {
                    Columns = ["ValueDate"],
                    Value = null,
                })
                .WriteToDelimited(new WriteToDelimitedMutator(context)
                {
                    Delimiter = ';',
                    WriteHeader = true,
                    Encoding = Encoding.UTF8,
                    FormatProvider = CultureInfo.InvariantCulture,
                    SinkProvider = new MemorySinkProvider()
                    {
                        StreamCreator = () => outputStream,
                        AutomaticallyDispose = false,
                    },
                    Columns = new()
                    {
                        ["Id"] = null,
                        ["Name"] = null,
                        ["Value1"] = new DelimitedColumn().FromSource("ValueString"),
                        ["Value2"] = new DelimitedColumn().FromSource("ValueInt"),
                        ["Value3"] = new DelimitedColumn().FromSource("ValueDate"),
                        ["Value4"] = new DelimitedColumn().FromSource("ValueDouble"),
                    },
                });

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(1, result.MutatedRows.Count);
            Assert.That.ExactMatch(result.MutatedRows, [
            new() { ["Id"] = 1, ["Name"] = " A", ["ValueString"] = "test\r\n continues", ["ValueInt"] = -1, ["ValueDate"] = null, ["ValueDouble"] = null } ]);

            Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);

            outputStream.Position = 0;
            var data = Encoding.UTF8.GetString(outputStream.ToArray());
            const string expected = "Id;Name;Value1;Value2;Value3;Value4\r\n1;\" A\";\"test\r\n continues\";-1;;\r\n";
            Assert.AreEqual(expected, data);
        }
    }
}
