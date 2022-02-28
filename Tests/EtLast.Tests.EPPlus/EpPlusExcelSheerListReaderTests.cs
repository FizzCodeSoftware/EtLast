namespace FizzCode.EtLast.Tests.EPPlus
{
    using System.Collections.Generic;
    using FizzCode.EtLast;
    using FizzCode.LightWeight.Collections;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class EpPlusExcelSheetReaderTests
    {
        private static EpPlusExcelSheetListReader GetReader(IEtlContext context, string fileName)
        {
            return new EpPlusExcelSheetListReader(context)
            {
                Source = new LocalFileStreamProvider()
                {
                    FileName = fileName,
                },
                AddRowIndexToColumn = "idx",
            };
        }

        [TestMethod]
        public void MissingFileThrowsFileReadException()
        {
            var context = TestExecuter.GetContext();
            var reader = GetReader(context, @".\TestData\MissingFile.xlsx");

            var builder = ProcessBuilder.Fluent
                .ReadFrom(reader)
                .ThrowExceptionOnRowError();

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(0, result.MutatedRows.Count);
            var exceptions = context.GetExceptions();
            Assert.AreEqual(1, exceptions.Count);
            Assert.IsTrue(exceptions[0] is LocalFileReadException);
        }

        [TestMethod]
        public void ListSheets()
        {
            var context = TestExecuter.GetContext();
            var reader = GetReader(context, @".\TestData\Test.xlsx");

            var builder = ProcessBuilder.Fluent
                .ReadFrom(reader)
                .ThrowExceptionOnRowError();

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(2, result.MutatedRows.Count);
            Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
                new CaseInsensitiveStringKeyDictionary<object>() { ["Index"] = 0, ["Name"] = "MergeAtIndex0", ["Color"] = System.Drawing.Color.FromArgb(0, 0, 0, 0), ["Visible"] = true, ["idx"] = 0 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["Index"] = 1, ["Name"] = "DateBroken", ["Color"] = System.Drawing.Color.FromArgb(0, 0, 0, 0), ["Visible"] = true, ["idx"] = 1 } });
            var exceptions = context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }
    }
}