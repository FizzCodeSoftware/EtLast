namespace FizzCode.EtLast.Tests.Unit
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class DelimitedFileReaderProcessTests
    {
        private IEvaluable GetReader(ITopic topic, string fileName)
        {
            return new DelimitedFileReaderProcess(topic, "DelimitedFileReaderProcess")
            {
                FileName = fileName,
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
                TreatEmptyStringAsNull = false,
            };
        }

        [TestMethod]
        public void CheckContent()
        {
            var topic = new Topic("test", new EtlContext());
            var process = new ProcessBuilder()
            {
                InputProcess = GetReader(topic, @"TestData\Sample.csv"),
                Mutators = new MutatorList()
                {
                    new ReplaceErrorWithValueMutator(topic, null)
                    {
                        Columns = new[] { "ValueDate" },
                        Value = null
                    },
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
        public void InvalidConversion()
        {
            var topic = new Topic("test", new EtlContext());
            var process = GetReader(topic, @"TestData\SampleInvalidConversion.csv");

            var result = process.Evaluate().TakeRowsAndReleaseOwnership().ToList();

            Assert.AreEqual(2, result.Count);
            Assert.That.RowsAreEqual(RowHelper.CreateRows(
                new object[] { "Id", new EtlRowError("X"), "Name", "A", "ValueString", "AAA", "ValueInt", -1, "ValueDate", new EtlRowError(""), "ValueDouble", new EtlRowError("") },
                new object[] { "Id", 1, "Name", "B", "ValueString", string.Empty, "ValueInt", 3, "ValueDate", new DateTime(2019, 04, 25), "ValueDouble", 1.234D })
                , result
            );
        }
    }
}
