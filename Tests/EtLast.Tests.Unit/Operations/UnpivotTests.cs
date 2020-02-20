namespace FizzCode.EtLast.Tests.Unit
{
    using System.Linq;
    using FizzCode.EtLast.Tests.Base;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class UnpivotTests
    {
        protected string[] SampleColumns { get; } = { "Id", "Name", "Cars", "Houses", "Kids" };

        protected object[][] SampleRows { get; } = {
                new object[] { 1, "A", 1, 1, 2 },
                new object[] { 2, "B", 2, 1, 3 },
        };

        [TestMethod]
        public void UnpivotTest()
        {
            var topic = new Topic("test", new EtlContext());

            var unpivotProcess = new ProcessBuilder()
            {
                InputProcess = new CreateRowsProcess(topic, "UnpivotGenerator")
                {
                    Columns = SampleColumns,
                    InputRows = SampleRows.ToList(),
                },
                Mutators = new MutatorList()
                {
                    new UnpivotMutator(topic, "UnpivotProcess")
                    {
                        FixColumns = new[] { "Id", "Name" },
                        NewColumnForDimension = "InventoryItem",
                        NewColumnForValue = "Value"
                    },
                },
            }.Build();

            var result = unpivotProcess.Evaluate().TakeRowsAndReleaseOwnership().ToList();
            Assert.AreEqual(6, result.Count);
            Assert.That.RowsAreEqual(RowHelper.CreateRows(
                new object[] { "Id", 1, "Name", "A", "InventoryItem", "Cars", "Value", 1 },
                new object[] { "Id", 1, "Name", "A", "InventoryItem", "Houses", "Value", 1 },
                new object[] { "Id", 1, "Name", "A", "InventoryItem", "Kids", "Value", 2 },
                new object[] { "Id", 2, "Name", "B", "InventoryItem", "Cars", "Value", 2 },
                new object[] { "Id", 2, "Name", "B", "InventoryItem", "Houses", "Value", 1 },
                new object[] { "Id", 2, "Name", "B", "InventoryItem", "Kids", "Value", 3 })
                , result);
        }
    }
}