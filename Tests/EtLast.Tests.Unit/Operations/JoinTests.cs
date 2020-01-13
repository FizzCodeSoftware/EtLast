namespace FizzCode.EtLast.Tests.Unit
{
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

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
            var context = new EtlContext();

            var leftProcess = new OperationHostProcess(context, "LeftProcess")
            {
                Configuration = new OperationHostProcessConfiguration()
                {
                    MainLoopDelay = 10,
                },
                InputProcess = new CreateRowsProcess(context, "LeftGenerator")
                {
                    Columns = SampleColumnsA,
                    InputRows = SampleRowsA.ToList(),
                },
            };

            leftProcess.AddOperation(new JoinOperation()
            {
                NoMatchAction = new MatchAction(MatchMode.Remove),
                RightProcess = new CreateRowsProcess(context, "RightGenerator")
                {
                    Columns = SampleColumnsB,
                    InputRows = SampleRowsB.ToList(),
                },
                LeftKeySelector = row => row.GetAs<int>("id").ToString("D", CultureInfo.InvariantCulture),
                RightKeySelector = row => row.GetAs<int>("fk").ToString("D", CultureInfo.InvariantCulture),
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
            Assert.IsTrue(!result.Any(x => x.GetAs<int>("id") == 3));
            Assert.IsTrue(!result.Any(x => x.GetAs<int>("id") == 4));
            Assert.IsTrue(!result.Any(x => x.GetAs<int>("id") == 5));
        }
    }
}