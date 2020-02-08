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
        private EpPlusExcelReaderProcess GetReader(EtlContext context, string fileName)
        {
            return new EpPlusExcelReaderProcess(context, "EpPlusExcelReaderProcess", null)
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
            var context = new EtlContext();
            var reader = GetReader(context, @".\TestData\Sample.xlsx");
            reader.SheetName = "Sheet1";

            var process = new MutatorBuilder()
            {
                InputProcess = reader,
                Mutators = new List<IMutator>()
                {
                    new ThrowExceptionOnRowErrorMutator(context, null, null),
                }
            }.BuildEvaluable();

            var resultCount = process.Evaluate().CountRows(null);
            Assert.AreEqual(2, resultCount);
        }

        [TestMethod]
        public void SheetIndex()
        {
            var context = new EtlContext();
            var reader = GetReader(context, @".\TestData\Sample.xlsx");
            reader.SheetIndex = 0;

            var process = new MutatorBuilder()
            {
                InputProcess = reader,
                Mutators = new List<IMutator>()
                {
                    new ThrowExceptionOnRowErrorMutator(context, null, null),
                }
            }.BuildEvaluable();

            var resultCount = process.Evaluate().CountRows(null);
            Assert.AreEqual(2, resultCount);
        }

        [TestMethod]
        public void CheckContent()
        {
            var context = new EtlContext();
            var reader = GetReader(context, @".\TestData\Sample.xlsx");
            reader.SheetName = "Sheet1";

            var process = new MutatorBuilder()
            {
                InputProcess = reader,
                Mutators = new List<IMutator>()
                {
                    new ThrowExceptionOnRowErrorMutator(context, null, null),
                }
            }.BuildEvaluable();

            var result = process.Evaluate().TakeRowsAndReleaseOwnership().ToList();
            Assert.AreEqual(2, result.Count);

            Assert.That.RowsAreEqual(RowHelper.CreateRows(
                new object[] { "Id", 0, "Name", "A", "ValueString", "AAA", "ValueInt", -1, "ValueDate", null },
                new object[] { "Id", 1, "Name", "B", "ValueString", string.Empty, "ValueInt", 3, "ValueDate", new DateTime(2019, 04, 25), "ValueDouble", 1.234D })
                , result
            );
        }
    }
}
