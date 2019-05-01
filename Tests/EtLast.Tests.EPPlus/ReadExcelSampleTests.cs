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
    public class ReadExcelSampleTests
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
                FileName = @".\TestData\Sample.xlsx",
                ColumnMap = new List<(string ExcelColumn, string RowColumn, ITypeConverter Converter, object ValueIfNull)>
                    {
                        ("Id", "Id", new IntConverter(), string.Empty),
                        ("Name", "Name", new StringConverter(), string.Empty),
                        ("Value1", "ValueString", new StringConverter(), string.Empty),
                        ("Value2", "ValueInt", new IntConverter(), null),
                        ("Value3", "ValueDate", new DateConverter(), null),
                        ("Value4", "ValueDouble", new DoubleConverter(), null)
                    }
            };

            _process = new OperationProcess(context, "EpPlusProcess")
            {
                Configuration = operationProcessConfiguration,
                InputProcess = _epPlusExcelReaderProcess
            };

            _process.AddOperation(new ThrowExceptionOnRowErrorOperation());
        }

        [TestMethod]
        public void SheetName()
        {
            _epPlusExcelReaderProcess.SheetName = "Sheet1";

            List<IRow> result = _process.Evaluate().ToList();
            Assert.AreEqual(2, result.Count);
        }

        [TestMethod]
        public void SheetIndex()
        {
            _epPlusExcelReaderProcess.SheetIndex = 0;

            List<IRow> result = _process.Evaluate().ToList();
            Assert.AreEqual(2, result.Count);
        }

        [TestMethod]
        public void CheckContent()
        {
            _epPlusExcelReaderProcess.SheetName = "Sheet1";

            List<IRow> result = _process.Evaluate().ToList();
            Assert.AreEqual(2, result.Count);

            Assert.That.RowsAreEqual(RowHelper.CreateRows(
                new object[] { "Id", 0, "Name", "A", "ValueString", "AAA", "ValueInt", -1, "ValueDate", null },
                new object[] { "Id", 1, "Name", "B", "ValueString", string.Empty, "ValueInt", 3, "ValueDate", new DateTime(2019, 04, 25), "ValueDouble", 1.234D })
                , result
            );
        }
    }
}
