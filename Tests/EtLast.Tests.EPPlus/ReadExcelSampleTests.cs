namespace FizzCode.EtLast.Tests.EPPlus
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using FizzCode.EtLast;
    using FizzCode.EtLast.EPPlus;
    using FizzCode.EtLast.Tests.Base;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class ReadExcelSampleTests
    {
        private EpPlusExcelReaderProcess GetReader(ITopic topic, string fileName)
        {
            return new EpPlusExcelReaderProcess(topic, "EpPlusExcelReaderProcess")
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
                }
            };
        }

        [TestMethod]
        public void SheetName()
        {
            var topic = new Topic("test", new EtlContext());

            var reader = GetReader(topic, @".\TestData\Sample.xlsx");
            reader.SheetName = "Sheet1";

            var process = new ProcessBuilder()
            {
                InputProcess = reader,
                Mutators = new MutatorList()
                {
                    new ThrowExceptionOnRowErrorMutator(topic),
                }
            }.Build();

            var resultCount = process.Evaluate().CountRows();
            Assert.AreEqual(2, resultCount);
        }

        [TestMethod]
        public void SheetIndex()
        {
            var topic = new Topic("test", new EtlContext());

            var reader = GetReader(topic, @".\TestData\Sample.xlsx");
            reader.SheetIndex = 0;

            var process = new ProcessBuilder()
            {
                InputProcess = reader,
                Mutators = new MutatorList()
                {
                    new ThrowExceptionOnRowErrorMutator(topic),
                }
            }.Build();

            var resultCount = process.Evaluate().CountRows();
            Assert.AreEqual(2, resultCount);
        }

        [TestMethod]
        public void CheckContent()
        {
            var topic = new Topic("test", new EtlContext());

            var reader = GetReader(topic, @".\TestData\Sample.xlsx");
            reader.SheetName = "Sheet1";

            var process = new ProcessBuilder()
            {
                InputProcess = reader,
                Mutators = new MutatorList()
                {
                    new ThrowExceptionOnRowErrorMutator(topic),
                }
            }.Build();

            var result = process.Evaluate().TakeRowsAndReleaseOwnership().ToList();
            Assert.AreEqual(2, result.Count);

            Assert.That.RowsAreEqual(RowHelper.CreateRows(
                new object[] { "Id", 0, "Name", "A", "ValueString", "AAA", "ValueInt", -1, "ValueDate", null },
                new object[] { "Id", 1, "Name", "B", "ValueString", string.Empty, "ValueInt", 3, "ValueDate", new DateTime(2019, 04, 25), "ValueDouble", 1.234D })
                , result
            );
        }

        [TestMethod]
        public void CheckContentNoTrim()
        {
            var topic = new Topic("test", new EtlContext());

            var reader = GetReader(topic, @".\TestData\Sample.xlsx");
            reader.SheetName = "Sheet1";
            reader.AutomaticallyTrimAllStringValues = false;

            var process = new ProcessBuilder()
            {
                InputProcess = reader,
                Mutators = new MutatorList()
                {
                    new ThrowExceptionOnRowErrorMutator(topic),
                }
            }.Build();

            var result = process.Evaluate().TakeRowsAndReleaseOwnership().ToList();
            Assert.AreEqual(2, result.Count);

            Assert.That.RowsAreEqual(RowHelper.CreateRows(
                new object[] { "Id", 0, "Name", "A", "ValueString", "AAA   ", "ValueInt", -1, "ValueDate", null },
                new object[] { "Id", 1, "Name", "B", "ValueString", string.Empty, "ValueInt", 3, "ValueDate", new DateTime(2019, 04, 25), "ValueDouble", 1.234D })
                , result
            );
        }
    }
}
