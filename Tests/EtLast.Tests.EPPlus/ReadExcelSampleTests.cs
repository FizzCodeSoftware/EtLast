namespace EtLast.Tests.EPPlus
{
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using FizzCode.EtLast;
    using FizzCode.EtLast.EPPlus;
    using System.Collections.Generic;
    using System.Linq;

    [TestClass]
    public class ReadExcelSampleTests
    {
        [TestMethod]
        public void TestMethod1()
        {
            var operationProcessConfiguration = new OperationProcessConfiguration()
            {
                WorkerCount = 2,
                MainLoopDelay = 10,
            };

            var context = new EtlContext<DictionaryRow>();

            var epPlusProcess = new OperationProcess(context, "EpPlusProcess")
            {
                Configuration = operationProcessConfiguration,
                InputProcess = new EpPlusExcelReaderProcess(context, "EpPlusExcelReaderProcess")
                {
                    FileName = @"..\..\TestData\Sample.xlsx",
                    ColumnMap = new List<(string ExcelColumn, string RowColumn, ITypeConverter Converter, object ValueIfNull)>
                    {
                        ("Id", "Id", new StringConverter(), string.Empty),
                        ("Name", "Name", new StringConverter(), string.Empty),
                        ("Value1", "ValueString", new StringConverter(), string.Empty),
                        ("Value2", "ValueInt", new IntConverter(), null),
                        ("Value3", "ValueDate", new DateConverter(), null),
                        ("Value4", "ValueDouble", new DoubleConverter(), null)
                    },
                    SheetName = "Sheet1"
                }
            };

            epPlusProcess.AddOperation(new ThrowExceptionOnRowErrorOperation());

            List<IRow> result = epPlusProcess.Evaluate().ToList();
            Assert.AreEqual(2, result.Count);
        }
    }
}
