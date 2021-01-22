namespace FizzCode.EtLast.Tests.EPPlus
{
    using System;
    using System.Collections.Generic;
    using FizzCode.EtLast;
    using FizzCode.LightWeight.Collections;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class ReadExcelReaderConversionTests
    {
        [TestMethod]
        public void WrapIsWorking()
        {
            var topic = TestExecuter.GetTopic();
            var builder = ProcessBuilder.Fluent
                .ReadFromExcel(new EpPlusExcelReader(topic, null)
                {
                    FileName = @".\TestData\Test.xlsx",
                    SheetName = "DateBroken",
                    ColumnConfiguration = new List<ReaderColumnConfiguration>()
                    {
                        new ReaderColumnConfiguration("Id", new IntConverter()),
                        new ReaderColumnConfiguration("Date", new DateConverter()),
                    },
                });

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(2, result.MutatedRows.Count);
            Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 0, ["Date"] = new EtlRowError(0d) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 1, ["Date"] = new DateTime(2019, 4, 25, 0, 0, 0, 0) } });
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }
    }
}