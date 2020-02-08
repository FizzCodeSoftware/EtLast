namespace FizzCode.EtLast.Tests.Unit
{
    using System.Linq;
    using FizzCode.EtLast.Tests.Base;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class RemoveColumnsOperationTests : AbstractBaseTestUsingSeed
    {
        [TestMethod]
        public void RemoveAll()
        {
            const int rowCount = 100;

            var context = new EtlContext();
            var process = CreateMutatorBuilder(rowCount, context);
            process.Mutators.Add(new RemoveColumnsMutator(context, null, null)
            {
                Columns = new[] { "id", "name", "age", "fkid", "date", "time", "datetime" },
            });

            var etl = RunEtl(process);
            var result = etl.Sum(x => x.ColumnCount);
            const int expected = 0;

            Assert.AreEqual(expected, result);
        }

        [TestMethod]
        public void RemoveSome()
        {
            const int rowCount = 100;

            var context = new EtlContext();
            var process = CreateMutatorBuilder(rowCount, context);
            process.Mutators.Add(new RemoveColumnsMutator(context, null, null)
            {
                Columns = new[] { "name", "fkid" },
            });

            var etl = RunEtl(process);
            var result = etl.Sum(x => x.ColumnCount);
            var expected = rowCount * 5;

            Assert.AreEqual(expected, result);
        }

        [TestMethod]
        public void RemovePrimaryKey()
        {
            const int rowCount = 100;

            var context = new EtlContext();
            var process = CreateMutatorBuilder(rowCount, context);
            process.Mutators.Add(new RemoveColumnsMutator(context, null, null)
            {
                Columns = new[] { "id" },
            });

            var etl = RunEtl(process);
            var result = etl.Sum(x => x.ColumnCount);
            var expected = rowCount * 6;

            Assert.AreEqual(expected, result);
        }
    }
}