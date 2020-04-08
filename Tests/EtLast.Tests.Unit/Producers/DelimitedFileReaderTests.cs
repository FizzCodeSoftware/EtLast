namespace FizzCode.EtLast.Tests.Unit.Producers
{
    using System;
    using System.Collections.Generic;
    using FizzCode.LightWeight.Collections;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class DelimitedFileReaderTests
    {
        private IEvaluable GetReader(ITopic topic, string fileName)
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
    }
}
