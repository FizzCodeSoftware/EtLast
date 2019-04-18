namespace FizzCode.EtLast.Tests.Unit
{
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using System.Linq;

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

            var etl = RunEtl(process);

            var result = etl.FirstOrDefault()[SampleColumns[1]];
            var expected = SampleRows.FirstOrDefault()[1];

            Assert.AreEqual(expected, result);

            result = etl.Skip(1).FirstOrDefault()[SampleColumns[3]];
            expected = SampleRows.Skip(1).FirstOrDefault()[3];

            Assert.AreEqual(expected, result);
        }
    }
}