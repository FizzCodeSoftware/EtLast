namespace FizzCode.EtLast.Tests.Unit
{
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using System.Collections.Generic;
    using System.Linq;

    [TestClass]
    public class JoinTests
    {
        protected string[] SampleColumnsA { get; } = { "id", "name", "age", "height" };
        protected object[][] SampleRowsA { get; } = {
                new object[] { 0, "A", 7, 160 },
                new object[] { 1, "B", 8, 190 },
                new object[] { 2, "C", 7, 170 },
                new object[] { 3, "A", 9, 160 },
                new object[] { 4, "A", 9, 160 },
                new object[] { 5, "B", 11, 140 },
        };

        protected string[] SampleColumnsB { get; } = { "id", "fk", "color" };
        protected object[][] SampleRowsB { get; } = {
                new object[] { 0, 0, "yellow" },
                new object[] { 1, 0, "red" },
                new object[] { 2, 0, "green" },
                new object[] { 3, 1, "blue" },
                new object[] { 4, 1, "yellow" },
                new object[] { 5, 2, "black" },
        };

        [TestMethod]
        public void InnerJoinTest()
        {
            var operationProcessConfiguration = new OperationProcessConfiguration()
            {
                WorkerCount = 2,
                MainLoopDelay = 10,
            };

            var context = new EtlContext<DictionaryRow>();

            var leftProcess = new OperationProcess(context, "LeftProcess")
            {
                Configuration = operationProcessConfiguration,
                InputProcess = new CreateRowsProcess(context, "LeftGenerator")
                {
                    Columns = SampleColumnsA,
                    InputRows = SampleRowsA.ToList(),
                },
            };

            leftProcess.AddOperation(new JoinOperation(NoMatchMode.RemoveIfNoMatch)
            {
                RightProcess = new CreateRowsProcess(context, "RightGenerator")
                {
                    Columns = SampleColumnsB,
                    InputRows = SampleRowsB.ToList(),
                },
                LeftKeySelector = row => row.GetAs<int>("id").ToString(),
                RightKeySelector = row => row.GetAs<int>("fk").ToString(),
                ColumnConfiguration = new List<ColumnCopyConfiguration>
                {
                    new ColumnCopyConfiguration("color"),
                }
            });

            var result = leftProcess.Evaluate().ToList();
            Assert.AreEqual(6, result.Count);
            Assert.IsTrue(result.Count(x => x.GetAs<string>("name") == "A") == 3);
            Assert.IsTrue(result.Count(x => x.GetAs<string>("name") == "B") == 2);
            Assert.IsTrue(result.Count(x => x.GetAs<string>("name") == "C") == 1);
            Assert.IsTrue(result.Count(x => x.GetAs<int>("id") == 3) == 0);
            Assert.IsTrue(result.Count(x => x.GetAs<int>("id") == 4) == 0);
            Assert.IsTrue(result.Count(x => x.GetAs<int>("id") == 5) == 0);
        }
    }
}