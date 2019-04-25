namespace EtLast.Tests.EPPlus
{
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using FizzCode.EtLast;
    using FizzCode.EtLast.EPPlus;

    [TestClass]
    public class UnitTest1
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
                    FileName = "1.xlsx",
                    // ColumnMap
                }
            };
        }
    }
}
