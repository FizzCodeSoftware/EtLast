namespace FizzCode.EtLast.Tests.Unit
{
    using System.Linq;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using FizzCode.EtLast.Tests.Base;

    [TestClass]
    public class CreateRowsProcessTests : AbstractBaseTestUsingSample
    {
        [TestMethod]
        public void RowCountIsTheSame()
        {
            var process = CreateProcess();

            var etl = RunEtl(process);
            var result = etl.Count();
            var expected = SampleRows.Length;

            Assert.AreEqual(expected, result);
        }

        [TestMethod]
        public void RowContentIsTheSame()
        {
            var process = CreateProcess();

            var result = RunEtl(process);

            var expected = RowHelper.CreateRows(SampleColumns, SampleRows);

            // Assert.That.Equals(expected, result);
            AssertExtensions.Equals(null, expected, result);
        }
    }
}