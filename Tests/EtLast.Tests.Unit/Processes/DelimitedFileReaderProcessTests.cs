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
        private IEvaluable GetReader(EtlContext context, string fileName)
        {
            return new DelimitedFileReaderProcess(context, "DelimitedFileReaderProcess", null)
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
            var context = new EtlContext();
            var process = new ProcessBuilder()
            {
                InputProcess = GetReader(context, @"TestData\Sample.csv"),
                Mutators = new MutatorList()
                {
                    new ReplaceErrorWithValueMutator(context, null, null)
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
            var context = new EtlContext();
            var process = GetReader(context, @"TestData\SampleInvalidConversion.csv");

            var result = process.Evaluate().TakeRowsAndReleaseOwnership().ToList();

            Assert.AreEqual(2, result.Count);
            Assert.That.RowsAreEqual(RowHelper.CreateRows(
                new object[] { "Id", new EtlRowErrorTest("X"), "Name", "A", "ValueString", "AAA", "ValueInt", -1, "ValueDate", new EtlRowErrorTest(""), "ValueDouble", new EtlRowErrorTest("") },
                new object[] { "Id", 1, "Name", "B", "ValueString", string.Empty, "ValueInt", 3, "ValueDate", new DateTime(2019, 04, 25), "ValueDouble", 1.234D })
                , result
            );
        }
    }
}
