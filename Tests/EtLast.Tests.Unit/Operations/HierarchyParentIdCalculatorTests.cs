namespace FizzCode.EtLast.Tests.Unit
{
    using System.Collections.Generic;
    using System.Linq;
    using FizzCode.EtLast.Tests.Base;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class HierarchyParentIdCalculatorTests
    {
        protected string[] SampleColumns { get; } = { "id", "name", "level1", "level2", "level3" };

        protected object[][] SampleRows { get; } = {
                new object[] { 0, "A", "AAA" },
                new object[] { 1, "B", null, "BBB" },
                new object[] { 2, "C", null, null, "CCC" },
                new object[] { 3, "D", null, null, "DDD" },
                new object[] { 4, "E", null, "EEE" },
                new object[] { 5, "F", null, "FFF" },
        };

        [TestMethod]
        public void HierarchyParentIdCalculatorTest()
        {
            var context = new EtlContext();

            var process = new MutatorBuilder()
            {
                InputProcess = new CreateRowsProcess(context, "HierarchyParentIdCalculatorGenerator", null)
                {
                    Columns = SampleColumns,
                    InputRows = SampleRows.ToList(),
                },
                Mutators = GetMutators(context).ToList(),
            };

            var result = process.BuildEvaluable().Evaluate().TakeRowsAndReleaseOwnership().ToList();
            var exceptions = context.GetExceptions();

            Assert.AreEqual(6, result.Count);
            Assert.AreEqual(0, exceptions.Count);
            Assert.That.RowsAreEqual(RowHelper.CreateRows(
                new object[] { "id", 0, "name", "A", "parentId", null, "level1", "AAA" },
                new object[] { "id", 1, "name", "B", "parentId", 0, "level1", null, "level2", "BBB" },
                new object[] { "id", 2, "name", "C", "parentId", 1, "level1", null, "level2", null, "level3", "CCC" },
                new object[] { "id", 3, "name", "D", "parentId", 1, "level1", null, "level2", null, "level3", "DDD" },
                new object[] { "id", 4, "name", "E", "parentId", 0, "level1", null, "level2", "EEE" },
                new object[] { "id", 5, "name", "F", "parentId", 0, "level1", null, "level2", "FFF" }), result);
        }

        private static IEnumerable<IMutator> GetMutators(EtlContext context)
        {
            yield return new HierarchyParentIdCalculatorMutator(context, null, null)
            {
                IntegerIdColumn = "id",
                NewColumnWithParentId = "parentId",
                LevelColumns = new[] { "level1", "level2", "level3" }
            };
        }
    }
}