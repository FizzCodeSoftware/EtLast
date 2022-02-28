namespace FizzCode.EtLast.Tests.Unit.Delimited
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Text;
    using FizzCode.LightWeight.Collections;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

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
                    ["Id"] = new ReaderColumnConfiguration(new IntConverter()),
                    ["Name"] = new ReaderColumnConfiguration(new StringConverter()),
                    ["ValueString"] = new ReaderColumnConfiguration(new StringConverter()).FromSource("Value1"),
                    ["ValueInt"] = new ReaderColumnConfiguration(new IntConverter()).FromSource("Value2"),
                    ["ValueDate"] = new ReaderColumnConfiguration(new DateConverter()).FromSource("Value3"),
                    ["ValueDouble"] = new ReaderColumnConfiguration(new DoubleConverter()).FromSource("Value4"),
                },
                HasHeader = true,
                RemoveSurroundingDoubleQuotes = removeSurroundingDoubleQuotes
            };
        }

        [TestMethod]
        public void PersonWriterWithReaderTest()
        {
            using (var outputStream = new MemoryStream())
            {
                var context = TestExecuter.GetContext();
                var builder = ProcessBuilder.Fluent
                    .ReadFrom(TestData.Person(context))
                    .WriteToDelimited(new WriteToDelimitedMutator(context)
                    {
                        SinkProvider = new MemorySinkProvider()
                        {
                            StreamCreator = () => outputStream,
                        },
                        WriteHeader = true,
                        Columns = new()
                        {
                            ["id"] = null,
                            ["name"] = null,
                            ["age"] = null,
                            ["HeightInCm"] = new DelimitedColumnConfiguration().FromSource("height"),
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
6;fake;;140;;5;2018.01.09 00:00:00.0000000;", data);

                outputStream.Position = 0;

                context = TestExecuter.GetContext();
                builder = ProcessBuilder.Fluent
                    .ReadDelimitedLines(new DelimitedLineReader(context)
                    {
                        StreamProvider = new CustomStreamProvider()
                        {
                            StreamCreator = () => outputStream,
                        },
                        HasHeader = true,
                        Columns = new()
                        {
                            ["id"] = new ReaderColumnConfiguration(new IntConverter()),
                            ["name"] = new ReaderColumnConfiguration(new StringConverter()),
                            ["age"] = new ReaderColumnConfiguration(new IntConverter()),
                            ["HeightInCm"] = new ReaderColumnConfiguration(new IntConverter()),
                            ["eyeColor"] = new ReaderColumnConfiguration(new StringConverter()),
                            ["countryId"] = new ReaderColumnConfiguration(new IntConverter()),
                            ["birthDate"] = new ReaderColumnConfiguration(new DateTimeConverter()),
                            ["lastChangedTime"] = new ReaderColumnConfiguration(new DateTimeConverter()),
                        },
                    });

                result = TestExecuter.Execute(builder);
                Assert.AreEqual(7, result.MutatedRows.Count);
                Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 0, ["name"] = "A", ["age"] = 17, ["HeightInCm"] = 160, ["eyeColor"] = "brown", ["countryId"] = 1, ["birthDate"] = new DateTime(2010, 12, 9, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 12, 0, 1, 0) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 1, ["name"] = "B", ["age"] = 8, ["HeightInCm"] = 190, ["countryId"] = 1, ["birthDate"] = new DateTime(2011, 2, 1, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 13, 2, 0, 0) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 2, ["name"] = "C", ["age"] = 27, ["HeightInCm"] = 170, ["eyeColor"] = "green", ["countryId"] = 2, ["birthDate"] = new DateTime(2014, 1, 21, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 11, 21, 17, 11, 58, 0) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 3, ["name"] = "D", ["age"] = 39, ["HeightInCm"] = 160, ["eyeColor"] = "fake", ["birthDate"] = new DateTime(2018, 7, 11, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2017, 8, 1, 4, 9, 1, 0) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 4, ["name"] = "E", ["age"] = -3, ["HeightInCm"] = 160, ["countryId"] = 1, ["lastChangedTime"] = new DateTime(2019, 1, 1, 23, 59, 59, 0) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 5, ["name"] = "A", ["age"] = 11, ["HeightInCm"] = 140, ["birthDate"] = new DateTime(2013, 5, 15, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2018, 1, 1, 0, 0, 0, 0) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 6, ["name"] = "fake", ["HeightInCm"] = 140, ["countryId"] = 5, ["birthDate"] = new DateTime(2018, 1, 9, 0, 0, 0, 0) } });
                var exceptions = context.GetExceptions();
                Assert.AreEqual(0, exceptions.Count);
            }
        }

        [TestMethod]
        public void NewLineTest()
        {
            using (var outputStream = new MemoryStream())
            {
                var context = TestExecuter.GetContext();
                var builder = ProcessBuilder.Fluent
                    .ReadDelimitedLines(GetReader(context, @"TestData\NewLineSample1.csv"))
                    .ReplaceErrorWithValue(new ReplaceErrorWithValueMutator(context)
                    {
                        Columns = new[] { "ValueDate" },
                        Value = null,
                    })
                    .WriteToDelimited(new WriteToDelimitedMutator(context)
                    {
                        SinkProvider = new MemorySinkProvider()
                        {
                            StreamCreator = () => outputStream,
                        },
                        WriteHeader = true,
                        Columns = new()
                        {
                            ["Id"] = null,
                            ["Name"] = null,
                            ["Value1"] = new DelimitedColumnConfiguration().FromSource("ValueString"),
                            ["Value2"] = new DelimitedColumnConfiguration().FromSource("ValueInt"),
                            ["Value3"] = new DelimitedColumnConfiguration().FromSource("ValueDate"),
                            ["Value4"] = new DelimitedColumnConfiguration().FromSource("ValueDouble"),
                        },
                    });

                var result = TestExecuter.Execute(builder);
                Assert.AreEqual(1, result.MutatedRows.Count);
                Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 1, ["Name"] = " A", ["ValueString"] = "test\r\n continues", ["ValueInt"] = -1 } });
                var exceptions = context.GetExceptions();
                Assert.AreEqual(0, exceptions.Count);

                outputStream.Position = 0;
                var data = Encoding.UTF8.GetString(outputStream.ToArray());
                var expected = "Id;Name;Value1;Value2;Value3;Value4\r\n1;\" A\";\"test\r\n continues\";-1;;";
                Assert.AreEqual(expected, data);
            }
        }
    }
}