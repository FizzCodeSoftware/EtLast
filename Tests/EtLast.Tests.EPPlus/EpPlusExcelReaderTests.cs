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
        private static EpPlusExcelReader GetReader(IEtlContext context, string fileName, string sheetName = null, int sheetIndex = -1, bool automaticallyTrimAllStringValues = true)
        {
            return new EpPlusExcelReader(context, null, null)
            {
                FileName = fileName,
                ColumnConfiguration = new()
                {
                    ["Id"] = new ReaderColumnConfiguration(new IntConverter(), NullSourceHandler.SetSpecialValue) { SpecialValueIfSourceIsNull = string.Empty },
                    ["Name"] = new ReaderColumnConfiguration(new StringConverter(), NullSourceHandler.SetSpecialValue) { SpecialValueIfSourceIsNull = string.Empty },
                    ["Value1"] = new ReaderColumnConfiguration("ValueString", new StringConverter(), NullSourceHandler.SetSpecialValue) { SpecialValueIfSourceIsNull = string.Empty },
                    ["Value2"] = new ReaderColumnConfiguration("ValueInt", new IntConverter()),
                    ["Value3"] = new ReaderColumnConfiguration("ValueDate", new DateConverter()),
                    ["Value4"] = new ReaderColumnConfiguration("ValueDouble", new DoubleConverter())
                },
                SheetName = sheetName,
                SheetIndex = sheetIndex,
                AutomaticallyTrimAllStringValues = automaticallyTrimAllStringValues,
            };
        }

        [TestMethod]
        public void ContentBySheetName()
        {
            var context = TestExecuter.GetContext();
            var reader = GetReader(context, @".\TestData\Test.xlsx", sheetName: "MergeAtIndex0");

            var builder = ProcessBuilder.Fluent
                .ReadFrom(reader)
                .ThrowExceptionOnRowError(new ThrowExceptionOnRowErrorMutator(context));

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(4, result.MutatedRows.Count);
            Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 0, ["Name"] = "A", ["ValueString"] = "AAA", ["ValueInt"] = -1},
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 1, ["Name"] = "B", ["ValueString"] = "AAA", ["ValueInt"] = 3, ["ValueDate"] = new DateTime(2019, 4, 12, 0, 0, 0, 0), ["ValueDouble"] = 1.234d },
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 2, ["Name"] = "C", ["ValueString"] = "C", ["ValueInt"] = 3, ["ValueDate"] = new DateTime(2019, 4, 25, 0, 0, 0, 0), ["ValueDouble"] = 1.234d },
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 3, ["Name"] = "X", ["ValueString"] = "X", ["ValueInt"] = 2, ["ValueDate"] = new DateTime(2019, 4, 25, 0, 0, 0, 0), ["ValueDouble"] = 98d } });
            var exceptions = context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void ContentBySheetIndex()
        {
            var context = TestExecuter.GetContext();
            var reader = GetReader(context, @".\TestData\Test.xlsx", sheetIndex: 0);

            var builder = ProcessBuilder.Fluent
                .ReadFrom(reader)
                .ThrowExceptionOnRowError(new ThrowExceptionOnRowErrorMutator(context));

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(4, result.MutatedRows.Count);
            Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 0, ["Name"] = "A", ["ValueString"] = "AAA", ["ValueInt"] = -1 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 1, ["Name"] = "B", ["ValueString"] = "AAA", ["ValueInt"] = 3, ["ValueDate"] = new DateTime(2019, 4, 12, 0, 0, 0, 0), ["ValueDouble"] = 1.234d },
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 2, ["Name"] = "C", ["ValueString"] = "C", ["ValueInt"] = 3, ["ValueDate"] = new DateTime(2019, 4, 25, 0, 0, 0, 0), ["ValueDouble"] = 1.234d },
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 3, ["Name"] = "X", ["ValueString"] = "X", ["ValueInt"] = 2, ["ValueDate"] = new DateTime(2019, 4, 25, 0, 0, 0, 0), ["ValueDouble"] = 98d } });
            var exceptions = context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void NoTrim()
        {
            var context = TestExecuter.GetContext();
            var reader = GetReader(context, @".\TestData\Test.xlsx", sheetName: "MergeAtIndex0", automaticallyTrimAllStringValues: false);

            var builder = ProcessBuilder.Fluent
                .ReadFrom(reader)
                .ThrowExceptionOnRowError(new ThrowExceptionOnRowErrorMutator(context));

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(4, result.MutatedRows.Count);
            Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 0, ["Name"] = "A   ", ["ValueString"] = "AAA", ["ValueInt"] = -1,},
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 1, ["Name"] = "B", ["ValueString"] = "AAA", ["ValueInt"] = 3, ["ValueDate"] = new DateTime(2019, 4, 12, 0, 0, 0, 0), ["ValueDouble"] = 1.234d },
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 2, ["Name"] = "C", ["ValueString"] = "C", ["ValueInt"] = 3, ["ValueDate"] = new DateTime(2019, 4, 25, 0, 0, 0, 0), ["ValueDouble"] = 1.234d },
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 3, ["Name"] = "X", ["ValueString"] = "X", ["ValueInt"] = 2, ["ValueDate"] = new DateTime(2019, 4, 25, 0, 0, 0, 0), ["ValueDouble"] = 98d } });
            var exceptions = context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }
    }
}