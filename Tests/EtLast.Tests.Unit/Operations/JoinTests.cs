namespace FizzCode.EtLast.Tests.Unit
{
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class JoinTests
    {
        protected string[] SampleColumnsA { get; } = { "id", "name", "age", "height", "color" };

        protected object[][] SampleRowsA { get; } = {
                new object[] { 0, "A", 7, 160, null },
                new object[] { 1, "B", 8, 190, null, },
                new object[] { 2, "C", 7, 170, "green" },
                new object[] { 3, "A", 9, 160, "unused" },
                new object[] { 4, "A", 9, 160, null },
                new object[] { 5, "B", 11, 140, null },
        };

        protected string[] SampleColumnsB { get; } = { "id", "fk", "color" };

        protected object[][] SampleRowsB { get; } = {
                new object[] { 0, 0, "yellow" },
                new object[] { 1, 0, "red" },
                new object[] { 2, 0, "green" },
                new object[] { 3, 1, "blue" },
                new object[] { 4, 1, "yellow" },
                new object[] { 5, 2, "black" },
                new object[] { 6, 100, "unused" },
        };

        [TestMethod]
        public void JoinMutatorTest()
        {
            var topic = new Topic("test", new EtlContext());

            var process = new ProcessBuilder()
            {
                InputProcess = new CreateRowsProcess(topic, "DataGenerator")
                {
                    Columns = SampleColumnsA,
                    InputRows = SampleRowsA.ToList(),
                },
                Mutators = new MutatorList()
                {
                    new JoinMutator(topic, "Joiner")
                    {
                        NoMatchAction = new NoMatchAction(MatchMode.Remove),
                        RightProcess = new CreateRowsProcess(topic, "RightGenerator")
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
                    }
                },
            }.Build();

            var result = process.Evaluate().TakeRowsAndReleaseOwnership().ToList();
            Assert.AreEqual(6, result.Count);
            Assert.IsTrue(result.Count(x => x.GetAs<string>("color") == "yellow") == 2);
            Assert.IsTrue(result.Count(x => x.GetAs<string>("color") == "red") == 1);
            Assert.IsTrue(result.Count(x => x.GetAs<string>("color") == "green") == 1);
            Assert.IsTrue(result.Count(x => x.GetAs<string>("color") == "blue") == 1);
            Assert.IsTrue(result.Count(x => x.GetAs<string>("color") == "black") == 1);
            Assert.IsTrue(!result.Any(x => x.GetAs<string>("color") == "unused"));
            Assert.IsTrue(result.Count(x => x.GetAs<string>("name") == "A") == 3);
            Assert.IsTrue(result.Count(x => x.GetAs<string>("name") == "B") == 2);
            Assert.IsTrue(result.Count(x => x.GetAs<string>("name") == "C") == 1);
            Assert.IsTrue(!result.Any(x => x.GetAs<int>("id") == 3));
            Assert.IsTrue(!result.Any(x => x.GetAs<int>("id") == 4));
            Assert.IsTrue(!result.Any(x => x.GetAs<int>("id") == 5));
        }

        [TestMethod]
        public void BatchedJoinMutatorTest()
        {
            var topic = new Topic("test", new EtlContext());

            var executedBatchCount = 0;

            var process = new ProcessBuilder()
            {
                InputProcess = new CreateRowsProcess(topic, "DataGenerator")
                {
                    Columns = SampleColumnsA,
                    InputRows = SampleRowsA.ToList(),
                },
                Mutators = new MutatorList()
                {
                    new BatchedJoinMutator(topic, "Joiner")
                    {
                        NoMatchAction = new NoMatchAction(MatchMode.Remove),
                        BatchSize = 4,
                        RightProcessCreator = rows =>
                        {
                            executedBatchCount++;
                            return new CreateRowsProcess(topic, "RightGenerator")
                            {
                                Columns = SampleColumnsB,
                                InputRows = SampleRowsB
                                    .Where(right => rows.Any(left => left.GetAs<int>("id") == (int)right[1]))
                                    .ToList(),
                            };
                        },
                        LeftKeySelector = row => row.GetAs<int>("id").ToString("D", CultureInfo.InvariantCulture),
                        RightKeySelector = row => row.GetAs<int>("fk").ToString("D", CultureInfo.InvariantCulture),
                        ColumnConfiguration = new List<ColumnCopyConfiguration>
                        {
                            new ColumnCopyConfiguration("color"),
                        }
                    }
                },
            }.Build();

            var result = process.Evaluate().TakeRowsAndReleaseOwnership().ToList();
            Assert.AreEqual(6, result.Count);
            Assert.AreEqual(2, executedBatchCount);
            Assert.IsTrue(result.Count(x => x.GetAs<string>("color") == "yellow") == 2);
            Assert.IsTrue(result.Count(x => x.GetAs<string>("color") == "red") == 1);
            Assert.IsTrue(result.Count(x => x.GetAs<string>("color") == "green") == 1);
            Assert.IsTrue(result.Count(x => x.GetAs<string>("color") == "blue") == 1);
            Assert.IsTrue(result.Count(x => x.GetAs<string>("color") == "black") == 1);
            Assert.IsTrue(!result.Any(x => x.GetAs<string>("color") == "unused"));
            Assert.IsTrue(result.Count(x => x.GetAs<string>("name") == "A") == 3);
            Assert.IsTrue(result.Count(x => x.GetAs<string>("name") == "B") == 2);
            Assert.IsTrue(result.Count(x => x.GetAs<string>("name") == "C") == 1);
            Assert.IsTrue(!result.Any(x => x.GetAs<int>("id") == 3));
            Assert.IsTrue(!result.Any(x => x.GetAs<int>("id") == 4));
            Assert.IsTrue(!result.Any(x => x.GetAs<int>("id") == 5));
        }
    }
}