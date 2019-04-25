namespace FizzCode.EtLast.Tests.Unit
{
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using System.Linq;

    [TestClass]
    public class RemoveRowOperationTests : AbstractBaseTestUsingSeed
    {
        [TestMethod]
        public void RemoveAll()
        {
            var rowCount = 1000;

            var process = CreateProcess();
            process.AddOperation(new RemoveRowOperation
            {
                If = row => true,
            });

            var etl = RunEtl(process, rowCount);
            var result = etl.Count();
            var expected = 0;

            Assert.AreEqual(expected, result);
        }

        [TestMethod]
        public void RemoveNone()
        {
            var rowCount = 1000;

            var process = CreateProcess();
            process.AddOperation(new RemoveRowOperation
            {
                If = row => false,
            });

            var etl = RunEtl(process, rowCount);
            var result = etl.Count();
            var expected = rowCount;

            Assert.AreEqual(expected, result);
        }

        [TestMethod]
        public void RemoveSome()
        {
            var rowCount = 1000;
            var keepAbove = 200;

            var process = CreateProcess();

            process.AddOperation(new RemoveRowOperation()
            {
                If = row => (int)row["id"] < keepAbove,
            });

            var etl = RunEtl(process, rowCount);
            var result = etl.Count();
            var expected = rowCount - keepAbove;

            Assert.AreEqual(expected, result);
        }
    }
}