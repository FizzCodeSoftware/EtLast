namespace FizzCode.EtLast.Tests.Unit
{
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
        public void OnlyOrderedOperationProcess()
        {
            var context = new EtlContext<DictionaryRow>();

            var hierarchyParentIdCalculatorProcess = new OperationProcess(context, "HierarchyParentIdCalculatorProcess")
            {
                Configuration = new OperationProcessConfiguration()
                {
                    MainLoopDelay = 10,
                },
                InputProcess = new CreateRowsProcess(context, "HierarchyParentIdCalculatorGenerator")
                {
                    Columns = SampleColumns,
                    InputRows = SampleRows.ToList(),
                }
            };

            AddOperation(hierarchyParentIdCalculatorProcess);

            var result = hierarchyParentIdCalculatorProcess.Evaluate().ToList();
            var exceptions = hierarchyParentIdCalculatorProcess.Context.GetExceptions();

            Assert.IsTrue(exceptions[0] is InvalidOperationParameterException);
            Assert.AreEqual(0, result.Count);
        }

        [TestMethod]
        public void HierarchyParentIdCalculatorTest()
        {
            var context = new EtlContext<DictionaryRow>();

            var hierarchyParentIdCalculatorProcess = new OperationProcess(context, "HierarchyParentIdCalculatorProcess")
            {
                Configuration = new OperationProcessConfiguration()
                {
                    MainLoopDelay = 10,
                    KeepOrder = true,
                },
                InputProcess = new CreateRowsProcess(context, "HierarchyParentIdCalculatorGenerator")
                {
                    Columns = SampleColumns,
                    InputRows = SampleRows.ToList(),
                }
            };

            AddOperation(hierarchyParentIdCalculatorProcess);

            var result = hierarchyParentIdCalculatorProcess.Evaluate().ToList();
            var exceptions = hierarchyParentIdCalculatorProcess.Context.GetExceptions();

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

        private static void AddOperation(IOperationProcess operationProcess)
        {
            operationProcess.AddOperation(new HierarchyParentIdCalculatorOperation()
            {
                IntegerIdColumn = "id",
                NewColumnWithParentId = "parentId",
                LevelColumns = new[] { "level1", "level2", "level3" }
            });
        }
    }
}