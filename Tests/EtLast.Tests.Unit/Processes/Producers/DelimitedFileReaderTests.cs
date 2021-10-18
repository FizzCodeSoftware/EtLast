namespace FizzCode.EtLast.Tests.Unit.Producers
{
    using System;
    using System.Collections.Generic;
    using FizzCode.LightWeight.Collections;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class DelimitedFileReaderTests
    {
        private static IEvaluable GetReader(ITopic topic, string fileName, bool removeSurroundingDoubleQuotes = true)
        {
            return new DelimitedFileReader(topic, null)
            {
                FileName = fileName,
                ColumnConfiguration = new List<ReaderColumnConfiguration>()
                {
                    new ReaderColumnConfiguration("Id", new IntConverter()),
                    new ReaderColumnConfiguration("Name", new StringConverter()),
                    new ReaderColumnConfiguration("Value1", "ValueString", new StringConverter()),
                    new ReaderColumnConfiguration("Value2", "ValueInt", new IntConverter()),
                    new ReaderColumnConfiguration("Value3", "ValueDate", new DateConverter()),
                    new ReaderColumnConfiguration("Value4", "ValueDouble", new DoubleConverter())
                },
                HasHeaderRow = true,
                RemoveSurroundingDoubleQuotes = removeSurroundingDoubleQuotes
            };
        }

        private static IEvaluable GetSimpleReader(ITopic topic, string fileName)
        {
            return new DelimitedFileReader(topic, null)
            {
                FileName = fileName,
                ColumnConfiguration = new List<ReaderColumnConfiguration>()
                {
                    new ReaderColumnConfiguration("Id", new IntConverter()),
                    new ReaderColumnConfiguration("Name", new StringConverter()),
                    new ReaderColumnConfiguration("Value", new StringConverter())
                },
                HasHeaderRow = true,
            };
        }

        [TestMethod]
        public void BasicTest()
        {
            var topic = TestExecuter.GetTopic();
            var builder = ProcessBuilder.Fluent
                .ReadFrom(GetReader(topic, @"TestData\Sample.csv"))
                .ReplaceErrorWithValue(new ReplaceErrorWithValueMutator(topic, null)
                {
                    Columns = new[] { "ValueDate" },
                    Value = null,
                });

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(2, result.MutatedRows.Count);
            Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 0, ["Name"] = "A", ["ValueString"] = "AAA", ["ValueInt"] = -1 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 1, ["Name"] = "B", ["ValueInt"] = 3, ["ValueDate"] = new DateTime(2019, 4, 25, 0, 0, 0, 0), ["ValueDouble"] = 1.234d } });
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void QuotedTest1()
        {
            var topic = TestExecuter.GetTopic();
            var builder = ProcessBuilder.Fluent
                .ReadFrom(GetReader(topic, @"TestData\QuotedSample1.csv"))
                .ReplaceErrorWithValue(new ReplaceErrorWithValueMutator(topic, null)
                {
                    Columns = new[] { "ValueDate" },
                    Value = null,
                });

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(2, result.MutatedRows.Count);
            Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 1, ["Name"] = "A", ["ValueString"] = "te\"s\"t;test", ["ValueInt"] = -1 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 2, ["Name"] = "tes\"t;t\"est", ["ValueInt"] = -1 } });
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void QuotedTest1KeepSurroundingDoubleQuotes()
        {
            var topic = TestExecuter.GetTopic();
            var builder = ProcessBuilder.Fluent
                .ReadFrom(GetReader(topic, @"TestData\QuotedSample1.csv", removeSurroundingDoubleQuotes: false))
                .ThrowExceptionOnRowError(new ThrowExceptionOnRowErrorMutator(topic));

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(2, result.MutatedRows.Count);
            Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 1, ["Name"] = "A", ["ValueString"] = "\"te\"s\"t;test\"", ["ValueInt"] = -1 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 2, ["Name"] = "\"tes\"t;t\"est\"", ["ValueInt"] = -1 } });
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void QuotedTest2()
        {
            var topic = TestExecuter.GetTopic();
            var builder = ProcessBuilder.Fluent
                .ReadFrom(GetSimpleReader(topic, @"TestData\QuotedSample2.csv"))
                .ReplaceErrorWithValue(new ReplaceErrorWithValueMutator(topic, null)
                {
                    Columns = new[] { "ValueDate" },
                    Value = null,
                });

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(3, result.MutatedRows.Count);
            Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 1, ["Name"] = "A", ["Value"] = "test" },
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 2, ["Name"] = "B", ["Value"] = "test\"" },
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 3, ["Name"] = "C", ["Value"] = "test\"\"" } });
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void NewLineTest1()
        {
            var topic = TestExecuter.GetTopic();
            var builder = ProcessBuilder.Fluent
                .ReadFrom(GetReader(topic, @"TestData\NewLineSample1.csv"))
                .ReplaceErrorWithValue(new ReplaceErrorWithValueMutator(topic, null)
                {
                    Columns = new[] { "ValueDate" },
                    Value = null,
                });

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(1, result.MutatedRows.Count);
            Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 1, ["Name"] = "A", ["ValueString"] = "test\n continues", ["ValueInt"] = -1 } });
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }


        [TestMethod]
        public void NewLineTest2()
        {
            var topic = TestExecuter.GetTopic();
            var builder = ProcessBuilder.Fluent
                .ReadFrom(GetReader(topic, @"TestData\NewLineSample2.csv"))
                .ReplaceErrorWithValue(new ReplaceErrorWithValueMutator(topic, null)
                {
                    Columns = new[] { "ValueDate" },
                    Value = null,
                });

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(1, result.MutatedRows.Count);
            Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 1, ["Name"] = "A", ["ValueString"] = "test\"\ncontinues", ["ValueInt"] = -1 } });
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void InvalidConversion()
        {
            var topic = TestExecuter.GetTopic();
            var builder = ProcessBuilder.Fluent
                .ReadFrom(GetReader(topic, @"TestData\SampleInvalidConversion.csv"))
                .ReplaceErrorWithValue(new ReplaceErrorWithValueMutator(topic, null)
                {
                    Columns = new[] { "ValueDate" },
                    Value = null,
                });

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(2, result.MutatedRows.Count);
            Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = new EtlRowError("X"), ["Name"] = "A", ["ValueString"] = "AAA", ["ValueInt"] = -1 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 1, ["Name"] = "B", ["ValueInt"] = 3, ["ValueDate"] = new DateTime(2019, 4, 25, 0, 0, 0, 0), ["ValueDouble"] = 1.234d } });
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }
    }
}
