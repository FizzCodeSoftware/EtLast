namespace EtLast.Tests.EPPlus
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using FizzCode.EtLast;
    using FizzCode.EtLast.EPPlus;
    using FizzCode.EtLast.Tests.Base;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class ReadExcelSampleErrorsTests
    {
        private IOperationProcess _process;
        private EpPlusExcelReaderProcess _epPlusExcelReaderProcess;

        [TestInitialize]
        public void Initialize()
        {
            var context = new EtlContext<DictionaryRow>();

            _epPlusExcelReaderProcess = new EpPlusExcelReaderProcess(context, "EpPlusExcelReaderProcess")
            {
                FileName = @"..\..\TestData\SampleErrors.xlsx",
                ColumnConfiguration = new List<ReaderColumnConfiguration>()
                    {
                        new ReaderColumnConfiguration("Id", new IntConverter(), NullSourceHandler.SetSpecialValue) { SpecialValueIfSourceIsNull =  string.Empty },
                        new ReaderColumnConfiguration("Date", new DateConverter()),
                    }
            };

            _process = new OperationProcess(context, "EpPlusProcess")
            {
                Configuration = new OperationProcessConfiguration()
                {
                    MainLoopDelay = 10,
                },
                InputProcess = _epPlusExcelReaderProcess
            };
        }

        [TestMethod]
        public void CheckContent()
        {
            _epPlusExcelReaderProcess.SheetName = "Date0";

            var result = _process.Evaluate().ToList();
            Assert.AreEqual(2, result.Count);

            Assert.That.RowsAreEqual(RowHelper.CreateRows(
                new object[] { "Id", 0, "Date", new EtlRowErrorTest(0D) }, // EPPLUS will provide value as double
                new object[] { "Id", 1, "Date", new DateTime(2019, 04, 25) })
                , result
            );
        }
    }
}