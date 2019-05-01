namespace FizzCode.EtLast.Tests.Unit
{
    using System;
    using System.Linq;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using FizzCode.EtLast.Tests.Base;

    [TestClass]
    public class SeedRowsProcessTests : AbstractBaseTestUsingSeed
    {
        [TestMethod]
        public void PrimaryKeyColumnIsSequentialInteger()
        {
            var rowCount = 100;

            var process = CreateProcess();

            var etl = RunEtl(process, rowCount);
            var result = etl.Select(row => (int)row["id"]);
            var expected = Enumerable.Range(0, rowCount);

            Assert.IsTrue(expected.SequenceEqual(result));
        }

        [TestMethod]
        public void ForeignKeyColumnIsInteger()
        {
            var rowCount = 100;

            var process = CreateProcess();

            var etl = RunEtl(process, rowCount);
            var result = etl.Select(row => (int)row["fkid"]).Count();
            var expected = rowCount;

            Assert.AreEqual(expected, result);
        }

        [TestMethod]
        public void NameColumnIsString()
        {
            var rowCount = 100;

            var process = CreateProcess();

            var etl = RunEtl(process, rowCount);
            var result = etl.Select(row => (string)row["name"]).Count(name => !string.IsNullOrEmpty(name));
            var expected = rowCount;

            Assert.AreEqual(expected, result);
        }

        [TestMethod]
        public void DateColumnIsDate()
        {
            var rowCount = 100;

            var process = CreateProcess();

            var etl = RunEtl(process, rowCount);
            var result = etl.Select(row => (DateTime)row["date"]).Count(dt => dt.Hour == 0);
            var expected = rowCount;

            Assert.AreEqual(expected, result);
        }

        [TestMethod]
        public void DateTimeColumnIsDateTime()
        {
            var rowCount = 100;

            var process = CreateProcess();

            var etl = RunEtl(process, rowCount);
            var result = etl.Select(row => (DateTime)row["datetime"]).Count(dt => dt.Year > 0);
            var expected = rowCount;

            Assert.AreEqual(expected, result);
        }

        [TestMethod]
        public void TimeColumnIsTimeSpan()
        {
            var rowCount = 100;

            var process = CreateProcess();

            var etl = RunEtl(process, rowCount);
            var result = etl.Select(row => (TimeSpan)row["time"]).Count();
            var expected = rowCount;

            Assert.AreEqual(expected, result);
        }

        [TestMethod]
        public void RowCount()
        {
            var rowCount = 100;

            var process = CreateProcess();

            var etl = RunEtl(process, rowCount);
            var result = etl.Count();
            var expected = rowCount;

            Assert.AreEqual(expected, result);
        }
    }
}