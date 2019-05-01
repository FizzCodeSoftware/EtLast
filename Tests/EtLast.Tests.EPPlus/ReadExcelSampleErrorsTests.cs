namespace EtLast.Tests.EPPlus
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using FizzCode.EtLast;
    using FizzCode.EtLast.EPPlus;
    using FizzCode.EtLast.Tests.Base;
    
    [TestClass]
    public class ReadExcelSampleErrorsTests
    {
        private IOperationProcess _process;
        private EpPlusExcelReaderProcess _epPlusExcelReaderProcess;

        [TestInitialize]
        public void Initialize()
        {
            var operationProcessConfiguration = new OperationProcessConfiguration()
            {
                WorkerCount = 2,
                MainLoopDelay = 10,
            };

            var context = new EtlContext<DictionaryRow>();

            _epPlusExcelReaderProcess = new EpPlusExcelReaderProcess(context, "EpPlusExcelReaderProcess")
            {
                FileName = @".\TestData\SampleErrors.xlsx",
                ColumnMap = new List<(string ExcelColumn, string RowColumn, ITypeConverter Converter, object ValueIfNull)>
                    {
                        ("Id", "Id", new IntConverter(), string.Empty),
                        ("Date", "Date", new DateConverter(), null)
                    }
            };

            _process = new OperationProcess(context, "EpPlusProcess")
            {
                Configuration = operationProcessConfiguration,
                InputProcess = _epPlusExcelReaderProcess
            };
        }


        [TestMethod]
        public void CheckContent()
        {
             _epPlusExcelReaderProcess.SheetName = "Date0";

            List<IRow> result = _process.Evaluate().ToList();
            Assert.AreEqual(2, result.Count);

            Assert.That.RowsAreEqual(RowHelper.CreateRows(
                new object[] { "Id", 0, "Date", new EtlRowErrorTest(0D) }, // EPPLUS will provide value as double
                new object[] { "Id", 1, "Date", new DateTime(2019, 04, 25) })
                , result
            );
        }
    }
}
