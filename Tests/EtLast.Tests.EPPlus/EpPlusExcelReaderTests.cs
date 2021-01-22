namespace FizzCode.EtLast.Tests.EPPlus
{
    using System;
    using System.Collections.Generic;
    using FizzCode.EtLast;
    using FizzCode.LightWeight.Collections;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class EpPlusExcelReaderTests
    {
        private static EpPlusExcelReader GetReader(ITopic topic, string fileName, string sheetName = null, int sheetIndex = -1, bool automaticallyTrimAllStringValues = true)
        {
            return new EpPlusExcelReader(topic, null)
            {
                FileName = fileName,
                ColumnConfiguration = new List<ReaderColumnConfiguration>()
                {
                    new ReaderColumnConfiguration("Id", new IntConverter(), NullSourceHandler.SetSpecialValue) { SpecialValueIfSourceIsNull =  string.Empty },
                    new ReaderColumnConfiguration("Name", new StringConverter(), NullSourceHandler.SetSpecialValue) { SpecialValueIfSourceIsNull =  string.Empty },
                    new ReaderColumnConfiguration("Value1", "ValueString", new StringConverter(), NullSourceHandler.SetSpecialValue) { SpecialValueIfSourceIsNull =  string.Empty },
                    new ReaderColumnConfiguration("Value2", "ValueInt", new IntConverter()),
                    new ReaderColumnConfiguration("Value3", "ValueDate", new DateConverter()),
                    new ReaderColumnConfiguration("Value4", "ValueDouble", new DoubleConverter())
                },
                SheetName = sheetName,
                SheetIndex = sheetIndex,
                AutomaticallyTrimAllStringValues = automaticallyTrimAllStringValues,
            };
        }

        [TestMethod]
        public void ContentBySheetName()
        {
            var topic = TestExecuter.GetTopic();
            var reader = GetReader(topic, @".\TestData\Test.xlsx", sheetName: "MergeAtIndex0");

            var builder = ProcessBuilder.Fluent
                .ReadFrom(reader)
                .ThrowExceptionOnRowError(new ThrowExceptionOnRowErrorMutator(topic));

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(4, result.MutatedRows.Count);
            Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 0, ["Name"] = "A", ["ValueString"] = "AAA", ["ValueInt"] = -1},
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 1, ["Name"] = "B", ["ValueString"] = "AAA", ["ValueInt"] = 3, ["ValueDate"] = new DateTime(2019, 4, 12, 0, 0, 0, 0), ["ValueDouble"] = 1.234d },
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 2, ["Name"] = "C", ["ValueString"] = "C", ["ValueInt"] = 3, ["ValueDate"] = new DateTime(2019, 4, 25, 0, 0, 0, 0), ["ValueDouble"] = 1.234d },
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 3, ["Name"] = "X", ["ValueString"] = "X", ["ValueInt"] = 2, ["ValueDate"] = new DateTime(2019, 4, 25, 0, 0, 0, 0), ["ValueDouble"] = 98d } });
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void ContentBySheetIndex()
        {
            var topic = TestExecuter.GetTopic();
            var reader = GetReader(topic, @".\TestData\Test.xlsx", sheetIndex: 0);

            var builder = ProcessBuilder.Fluent
                .ReadFrom(reader)
                .ThrowExceptionOnRowError(new ThrowExceptionOnRowErrorMutator(topic));

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(4, result.MutatedRows.Count);
            Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 0, ["Name"] = "A", ["ValueString"] = "AAA", ["ValueInt"] = -1 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 1, ["Name"] = "B", ["ValueString"] = "AAA", ["ValueInt"] = 3, ["ValueDate"] = new DateTime(2019, 4, 12, 0, 0, 0, 0), ["ValueDouble"] = 1.234d },
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 2, ["Name"] = "C", ["ValueString"] = "C", ["ValueInt"] = 3, ["ValueDate"] = new DateTime(2019, 4, 25, 0, 0, 0, 0), ["ValueDouble"] = 1.234d },
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 3, ["Name"] = "X", ["ValueString"] = "X", ["ValueInt"] = 2, ["ValueDate"] = new DateTime(2019, 4, 25, 0, 0, 0, 0), ["ValueDouble"] = 98d } });
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void NoTrim()
        {
            var topic = TestExecuter.GetTopic();
            var reader = GetReader(topic, @".\TestData\Test.xlsx", sheetName: "MergeAtIndex0", automaticallyTrimAllStringValues: false);

            var builder = ProcessBuilder.Fluent
                .ReadFrom(reader)
                .ThrowExceptionOnRowError(new ThrowExceptionOnRowErrorMutator(topic));

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(4, result.MutatedRows.Count);
            Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 0, ["Name"] = "A   ", ["ValueString"] = "AAA", ["ValueInt"] = -1,},
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 1, ["Name"] = "B", ["ValueString"] = "AAA", ["ValueInt"] = 3, ["ValueDate"] = new DateTime(2019, 4, 12, 0, 0, 0, 0), ["ValueDouble"] = 1.234d },
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 2, ["Name"] = "C", ["ValueString"] = "C", ["ValueInt"] = 3, ["ValueDate"] = new DateTime(2019, 4, 25, 0, 0, 0, 0), ["ValueDouble"] = 1.234d },
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 3, ["Name"] = "X", ["ValueString"] = "X", ["ValueInt"] = 2, ["ValueDate"] = new DateTime(2019, 4, 25, 0, 0, 0, 0), ["ValueDouble"] = 98d } });
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }
    }
}