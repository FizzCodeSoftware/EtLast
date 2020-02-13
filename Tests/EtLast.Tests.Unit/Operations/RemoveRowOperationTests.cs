namespace FizzCode.EtLast.Tests.Unit
{
    using FizzCode.EtLast.Tests.Base;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class RemoveRowOperationTests : AbstractBaseTestUsingSeed
    {
        [TestMethod]
        public void RemoveAll()
        {
            const int rowCount = 1000;

            var context = new EtlContext();
            var process = CreateProcessBuilder(rowCount, context);
            process.Mutators.Add(new RemoveRowMutator(context, null, null)
            {
                If = row => true,
            });

            var etl = RunEtl(process);
            var result = etl.Count;
            const int expected = 0;

            Assert.AreEqual(expected, result);
        }

        [TestMethod]
        public void RemoveNone()
        {
            const int rowCount = 1000;

            var context = new EtlContext();
            var process = CreateProcessBuilder(rowCount, context);
            process.Mutators.Add(new RemoveRowMutator(context, null, null)
            {
                If = row => false,
            });

            var etl = RunEtl(process);
            var result = etl.Count;
            var expected = rowCount;

            Assert.AreEqual(expected, result);
        }

        [TestMethod]
        public void RemoveSome()
        {
            const int rowCount = 1000;
            const int keepAbove = 200;

            var context = new EtlContext();
            var process = CreateProcessBuilder(rowCount, context);
            process.Mutators.Add(new RemoveRowMutator(context, null, null)
            {
                If = row => (int)row["id"] < keepAbove,
            });

            var etl = RunEtl(process);
            var result = etl.Count;
            var expected = rowCount - keepAbove;

            Assert.AreEqual(expected, result);
        }
    }
}