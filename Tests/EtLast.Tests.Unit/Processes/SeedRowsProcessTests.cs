namespace FizzCode.EtLast.Tests.Unit
{
    using System;
    using System.Linq;
    using FizzCode.EtLast.Tests.Base;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class SeedRowsProcessTests : AbstractBaseTestUsingSeed
    {
        [TestMethod]
        public void PrimaryKeyColumnIsSequentialInteger()
        {
            const int rowCount = 100;

            var process = CreateProcess();

            var etl = RunEtl(process, rowCount);
            var result = etl.Select(row => (int)row["id"]);
            var expected = Enumerable.Range(0, rowCount);

            Assert.IsTrue(expected.SequenceEqual(result));
        }

        [TestMethod]
        public void ForeignKeyColumnIsInteger()
        {
            const int rowCount = 100;

            var process = CreateProcess();

            var etl = RunEtl(process, rowCount);
            var result = etl.Select(row => (int)row["fkid"]).Count();
            var expected = rowCount;

            Assert.AreEqual(expected, result);
        }

        [TestMethod]
        public void NameColumnIsString()
        {
            const int rowCount = 100;

            var process = CreateProcess();

            var etl = RunEtl(process, rowCount);
            var result = etl.Select(row => (string)row["name"]).Count(name => !string.IsNullOrEmpty(name));
            var expected = rowCount;

            Assert.AreEqual(expected, result);
        }

        [TestMethod]
        public void DateColumnIsDate()
        {
            const int rowCount = 100;

            var process = CreateProcess();

            var etl = RunEtl(process, rowCount);
            var result = etl.Select(row => (DateTime)row["date"]).Count(dt => dt.Hour == 0);
            var expected = rowCount;

            Assert.AreEqual(expected, result);
        }

        [TestMethod]
        public void DateTimeColumnIsDateTime()
        {
            const int rowCount = 100;

            var process = CreateProcess();

            var etl = RunEtl(process, rowCount);
            var result = etl.Select(row => (DateTime)row["datetime"]).Count(dt => dt.Year > 0);
            var expected = rowCount;

            Assert.AreEqual(expected, result);
        }

        [TestMethod]
        public void TimeColumnIsTimeSpan()
        {
            const int rowCount = 100;

            var process = CreateProcess();

            var etl = RunEtl(process, rowCount);
            var result = etl.Select(row => (TimeSpan)row["time"]).Count();
            var expected = rowCount;

            Assert.AreEqual(expected, result);
        }

        [TestMethod]
        public void RowCount()
        {
            const int rowCount = 100;

            var process = CreateProcess();

            var etl = RunEtl(process, rowCount);
            var result = etl.Count;
            var expected = rowCount;

            Assert.AreEqual(expected, result);
        }
    }
}