namespace FizzCode.EtLast.Tests.Unit
{
    using FizzCode.EtLast.Tests.Base;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class CreateRowsProcessTests : AbstractBaseTestUsingSample
    {
        [TestMethod]
        public void RowCountIsTheSame()
        {
            var process = CreateProcess();

            var etl = RunEtl(process);
            var result = etl.Count;
            var expected = SampleRows.Length;

            Assert.AreEqual(expected, result);
        }

        [TestMethod]
        public void RowContentIsTheSame()
        {
            var process = CreateProcess();

            var result = RunEtl(process);

            var expected = RowHelper.CreateRows(SampleColumns, SampleRows);

            Assert.That.RowsAreEqual(expected, result);
        }
    }
}