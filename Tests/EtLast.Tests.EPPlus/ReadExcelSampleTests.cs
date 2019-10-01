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
        private IOperationHostProcess _process;
        private EpPlusExcelReaderProcess _epPlusExcelReaderProcess;

        [TestInitialize]
        public void Initialize()
        {
            var context = new EtlContext<DictionaryRow>();

            _epPlusExcelReaderProcess = new EpPlusExcelReaderProcess(context, "EpPlusExcelReaderProcess")
            {
                FileName = @"TestData\Sample.xlsx",
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

            _process = new OperationHostProcess(context, "EpPlusProcess")
            {
                Configuration = new OperationHostProcessConfiguration()
                {
                    MainLoopDelay = 10,
                },
                InputProcess = _epPlusExcelReaderProcess
            };

            _process.AddOperation(new ThrowExceptionOnRowErrorOperation());
        }

        [TestMethod]
        public void SheetName()
        {
            _epPlusExcelReaderProcess.SheetName = "Sheet1";

            var result = _process.Evaluate().ToList();
            Assert.AreEqual(2, result.Count);
        }

        [TestMethod]
        public void SheetIndex()
        {
            _epPlusExcelReaderProcess.SheetIndex = 0;

            var result = _process.Evaluate().ToList();
            Assert.AreEqual(2, result.Count);
        }

        [TestMethod]
        public void CheckContent()
        {
            _epPlusExcelReaderProcess.SheetName = "Sheet1";

            var result = _process.Evaluate().ToList();
            Assert.AreEqual(2, result.Count);

            Assert.That.RowsAreEqual(RowHelper.CreateRows(
                new object[] { "Id", 0, "Name", "A", "ValueString", "AAA", "ValueInt", -1, "ValueDate", null },
                new object[] { "Id", 1, "Name", "B", "ValueString", string.Empty, "ValueInt", 3, "ValueDate", new DateTime(2019, 04, 25), "ValueDouble", 1.234D })
                , result
            );
        }
    }
}
