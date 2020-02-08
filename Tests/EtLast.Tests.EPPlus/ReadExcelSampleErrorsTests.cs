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
    public class ReadExcelSampleErrorsTests
    {
        private EpPlusExcelReaderProcess GetReader(EtlContext context, string fileName)
        {
            return new EpPlusExcelReaderProcess(context, "EpPlusExcelReaderProcess", null)
            {
                FileName = fileName,
                ColumnConfiguration = new List<ReaderColumnConfiguration>()
                {
                    new ReaderColumnConfiguration("Id", new IntConverter(), NullSourceHandler.SetSpecialValue) { SpecialValueIfSourceIsNull =  string.Empty },
                    new ReaderColumnConfiguration("Date", new DateConverter()),
                }
            };
        }

        [TestMethod]
        public void CheckContent()
        {
            var context = new EtlContext();
            var reader = GetReader(context, @".\TestData\SampleErrors.xlsx");
            reader.SheetName = "Date0";

            var process = reader;

            var result = process.Evaluate().TakeRowsAndReleaseOwnership().ToList();
            Assert.AreEqual(2, result.Count);

            Assert.That.RowsAreEqual(RowHelper.CreateRows(
                new object[] { "Id", 0, "Date", new EtlRowErrorTest(0D) }, // EPPLUS will provide value as double
                new object[] { "Id", 1, "Date", new DateTime(2019, 04, 25) })
                , result
            );
        }
    }
}