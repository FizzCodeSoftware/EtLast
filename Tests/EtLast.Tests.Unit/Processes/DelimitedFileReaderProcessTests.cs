namespace FizzCode.EtLast.Tests.Unit
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using FizzCode.EtLast.Tests.Base;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class DelimitedFileReaderProcessTests
    {
        private IOperationHostProcess _process;
        private DelimitedFileReaderProcess _delimitedFileReaderProcess;

        [TestInitialize]
        public void Initialize()
        {
            var context = new EtlContext();

            _delimitedFileReaderProcess = new DelimitedFileReaderProcess(context, "DelimitedFileReaderProcess")
            {
                FileName = @"TestData\Sample.csv",
                ColumnConfiguration = new List<ReaderColumnConfiguration>()
                {
                    new ReaderColumnConfiguration("Id", new IntConverter(), NullSourceHandler.SetSpecialValue) { SpecialValueIfSourceIsNull =  string.Empty },
                    new ReaderColumnConfiguration("Name", new StringConverter(), NullSourceHandler.SetSpecialValue) { SpecialValueIfSourceIsNull =  string.Empty },
                    new ReaderColumnConfiguration("Value1", "ValueString", new StringConverter(), NullSourceHandler.SetSpecialValue) { SpecialValueIfSourceIsNull =  string.Empty },
                    new ReaderColumnConfiguration("Value2", "ValueInt", new IntConverter()),
                    new ReaderColumnConfiguration("Value3", "ValueDate", new DateConverter()),
                    new ReaderColumnConfiguration("Value4", "ValueDouble", new DoubleConverter(true))
                },
                HasHeaderRow = true,
            };

            _process = new OperationHostProcess(context, "DelimitedFileReaderOperationProcess")
            {
                Configuration = new OperationHostProcessConfiguration()
                {
                    MainLoopDelay = 10,
                },
                InputProcess = _delimitedFileReaderProcess
            };
        }

        [TestMethod]
        public void CheckContent()
        {
            _process.AddOperation(new ReplaceErrorWithValueOperation()
            {
                Columns = new[] { "ValueDate" },
                Value = null
            });

            var result = _process.Evaluate().ToList();
            Assert.AreEqual(2, result.Count);

            Assert.That.RowsAreEqual(RowHelper.CreateRows(
                new object[] { "Id", 0, "Name", "A", "ValueString", "AAA", "ValueInt", -1, "ValueDate", null },
                new object[] { "Id", 1, "Name", "B", "ValueString", string.Empty, "ValueInt", 3, "ValueDate", new DateTime(2019, 04, 25), "ValueDouble", 1.234D })
                , result
            );
        }

        [TestMethod]
        public void InvalidConversion()
        {
            _delimitedFileReaderProcess.FileName = @"TestData\SampleInvalidConversion.csv";
            var result = _process.Evaluate().ToList();

            // TODO check exception

            Assert.AreEqual(2, result.Count);
            Assert.That.RowsAreEqual(RowHelper.CreateRows(
                new object[] { "Id", new EtlRowErrorTest("X"), "Name", "A", "ValueString", "AAA", "ValueInt", -1, "ValueDate", new EtlRowErrorTest(""), "ValueDouble", new EtlRowErrorTest("") },
                new object[] { "Id", 1, "Name", "B", "ValueString", string.Empty, "ValueInt", 3, "ValueDate", new DateTime(2019, 04, 25), "ValueDouble", 1.234D })
                , result
            );
        }
    }
}
