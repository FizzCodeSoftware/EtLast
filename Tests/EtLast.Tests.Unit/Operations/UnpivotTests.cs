namespace FizzCode.EtLast.Tests.Unit
{
    using System.Collections.Generic;
    using System.Linq;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using FizzCode.EtLast.Tests.Base;

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
            var operationProcessConfiguration = new OperationProcessConfiguration()
            {
                WorkerCount = 2,
                MainLoopDelay = 10,
            };

            var context = new EtlContext<DictionaryRow>();

            var unpivotProcess = new OperationProcess(context, "UnpivotProcess")
            {
                Configuration = operationProcessConfiguration,
                InputProcess = new CreateRowsProcess(context, "UnpivotGenerator")
                {
                    Columns = SampleColumns,
                    InputRows = SampleRows.ToList(),
                },
            };

            unpivotProcess.AddOperation(new UnpivotOperation()
            {
                FixColumns = new[] { "Id", "Name" },
                NewColumnForDimension = "InventoryItem",
                NewColumnForValue = "Value"
            });

            List<IRow> result = unpivotProcess.Evaluate().ToList();
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