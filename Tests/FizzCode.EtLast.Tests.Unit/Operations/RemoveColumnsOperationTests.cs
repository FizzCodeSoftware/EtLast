namespace FizzCode.EtLast.Tests.Unit
{
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using System.Linq;

    [TestClass]
    public class RemoveColumnsOperationTests : AbstractBaseTestUsingSeed
    {
        [TestMethod]
        public void RemoveAll()
        {
            var rowCount = 100;

            var process = CreateProcess();
            process.AddOperation(new RemoveColumnsOperation()
            {
                Columns = new[] { "id", "name", "age", "fkid", "date", "time", "datetime" },
            });

            var etl = RunEtl(process, rowCount);
            var result = etl.Sum(x => x.ColumnCount);
            var expected = 0;

            Assert.AreEqual(expected, result);
        }

        [TestMethod]
        public void RemoveSome()
        {
            var rowCount = 100;

            var process = CreateProcess();
            process.AddOperation(new RemoveColumnsOperation()
            {
                Columns = new[] { "name", "fkid" },
            });

            var etl = RunEtl(process, rowCount);
            var result = etl.Sum(x => x.ColumnCount);
            var expected = rowCount * 5;

            Assert.AreEqual(expected, result);
        }

        [TestMethod]
        public void RemovePrimaryKey()
        {
            var rowCount = 100;

            var process = CreateProcess();
            process.AddOperation(new RemoveColumnsOperation()
            {
                Columns = new[] { "id" },
            });

            var etl = RunEtl(process, rowCount);
            var result = etl.Sum(x => x.ColumnCount);
            var expected = rowCount * 6;

            Assert.AreEqual(expected, result);
        }
    }
}