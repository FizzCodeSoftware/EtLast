namespace FizzCode.EtLast.Tests.Unit.Producers
{
    using System;
    using System.Collections.Generic;
    using FizzCode.LightWeight.Collections;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class DelimitedFileReaderTests
    {
        private static IEvaluable GetReader(ITopic topic, string fileName, bool removeSurroundingDoubleQuotes = true, bool throwOnMissingDoubleQuoteClose = true)
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
                RemoveSurroundingDoubleQuotes = removeSurroundingDoubleQuotes,
                ThrowOnMissingDoubleQuoteClose = throwOnMissingDoubleQuoteClose
            };
        }

        [TestMethod]
        public void CheckContent()
        {
            var topic = TestExecuter.GetTopic();
            var builder = new ProcessBuilder()
            {
                InputProcess = GetReader(topic, @"TestData\Sample.csv"),
                Mutators = new MutatorList()
                {
                    new ReplaceErrorWithValueMutator(topic, null)
                    {
                        Columns = new[] { "ValueDate" },
                        Value = null,
                    },
                }
            };

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(2, result.MutatedRows.Count);
            Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 0, ["Name"] = "A", ["ValueString"] = "AAA", ["ValueInt"] = -1},
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 1, ["Name"] = "B", ["ValueInt"] = 3, ["ValueDate"] = new DateTime(2019, 4, 25, 0, 0, 0, 0), ["ValueDouble"] = 1.234d } });
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void InvalidConversion()
        {
            var topic = TestExecuter.GetTopic();
            var builder = new ProcessBuilder()
            {
                InputProcess = GetReader(topic, @"TestData\SampleInvalidConversion.csv"),
                Mutators = new MutatorList()
                {
                    new ReplaceErrorWithValueMutator(topic, null)
                    {
                        Columns = new[] { "ValueDate" },
                        Value = null,
                    },
                }
            };

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(2, result.MutatedRows.Count);
            Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = new EtlRowError("X"), ["Name"] = "A", ["ValueString"] = "AAA", ["ValueInt"] = -1},
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 1, ["Name"] = "B", ["ValueInt"] = 3, ["ValueDate"] = new DateTime(2019, 4, 25, 0, 0, 0, 0), ["ValueDouble"] = 1.234d } });
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void QuatedDelimiter_RemoveDoubleQuotes()
        {
            var topic = TestExecuter.GetTopic();
            var builder = new ProcessBuilder()
            {
                InputProcess = GetReader(topic, @"TestData\SampleQuatedDelimiter.csv"),
                Mutators = new MutatorList
                {
                    new ThrowExceptionOnRowErrorMutator(topic)
                }
            };

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(2, result.MutatedRows.Count);
            Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 0, ["Name"] = "A", ["ValueString"] = "te\"s\"t;test", ["ValueInt"] = -1},
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 0, ["Name"] = "tes\"t;t\"est", ["ValueInt"] = -1}
            });
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void QuatedDelimiter_KeepDoubleQuotes()
        {
            var topic = TestExecuter.GetTopic();
            var builder = new ProcessBuilder()
            {
                InputProcess = GetReader(topic, @"TestData\SampleQuatedDelimiter.csv", removeSurroundingDoubleQuotes: false),
                Mutators = new MutatorList
                {
                    new ThrowExceptionOnRowErrorMutator(topic)
                }
            };


            var result = TestExecuter.Execute(builder);

            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);

            Assert.AreEqual(2, result.MutatedRows.Count);
            Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 0, ["Name"] = "A", ["ValueString"] = "\"te\"s\"t;test\"", ["ValueInt"] = -1},
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 0, ["Name"] = "\"tes\"t;t\"est\"", ["ValueInt"] = -1}
            });
        }

        [TestMethod]
        public void QuatedDelimiter_DontThrowOnMissingClose()
        {
            var topic = TestExecuter.GetTopic();
            var builder = new ProcessBuilder()
            {
                InputProcess = GetReader(topic, @"TestData\SampleQuatedDelimiterMissingClose.csv", throwOnMissingDoubleQuoteClose: false),
                Mutators = new MutatorList
                {
                    new ThrowExceptionOnRowErrorMutator(topic)
                }
            };

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(2, result.MutatedRows.Count);
            Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 0, ["Name"] = "A", ["ValueString"] = "\"test;test;-1;" },
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 0, ["Name"] = "\"test;test;A;-1;" }
            });
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void QuatedDelimiter_ThrowOnMissingClose()
        {
            var topic = TestExecuter.GetTopic();
            var builder = new ProcessBuilder()
            {
                InputProcess = GetReader(topic, @"TestData\SampleQuatedDelimiterMissingClose.csv", throwOnMissingDoubleQuoteClose: true),
                Mutators = new MutatorList
                {
                    new ThrowExceptionOnRowErrorMutator(topic)
                }
            };

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(0, result.MutatedRows.Count);
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(1, exceptions.Count);
        }
    }
}
