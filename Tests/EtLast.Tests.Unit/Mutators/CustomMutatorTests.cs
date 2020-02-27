namespace FizzCode.EtLast.Tests.Unit
{
    using System.Collections.Generic;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class CustomMutatorTests
    {
        [TestMethod]
        public void ThrowsInvalidProcessParameterException()
        {
            Assert.That.ThrowsInvalidProcessParameterException<CustomMutator>();
        }

        [TestMethod]
        public void DelegateThrowsExceptionThen()
        {
            var invocationCount = 0;
            var topic = TestExecuter.GetTopic();
            var builder = new ProcessBuilder()
            {
                InputProcess = TestData.Person(topic),
                Mutators = new MutatorList()
                {
                    new CustomMutator(topic, null)
                    {
                        Then = (proc, row) =>
                        {
                            invocationCount++;
                            var x = row.GetAs<int>("x");
                            return true;
                        }
                    }
                },
            };

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(1, invocationCount);
            Assert.AreEqual(0, result.MutatedRows.Count);
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(1, exceptions.Count);
            Assert.IsTrue(exceptions[0] is ProcessExecutionException);
        }

        [TestMethod]
        public void RemoveRowsWithDelegate()
        {
            var topic = TestExecuter.GetTopic();
            var builder = new ProcessBuilder()
            {
                InputProcess = TestData.Person(topic),
                Mutators = new MutatorList()
                {
                    new CustomMutator(topic, null)
                    {
                        Then = (proc, row) =>
                        {
                            return row.GetAs<int>("id") < 4;
                        }
                    }
                },
            };

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(4, result.MutatedRows.Count);
            Assert.That.OrderedMatch(result, new List<Dictionary<string, object>>() {
                new Dictionary<string, object>() { ["id"] = 0, ["name"] = "A" },
                new Dictionary<string, object>() { ["id"] = 1, ["name"] = "B" },
                new Dictionary<string, object>() { ["id"] = 2, ["name"] = "C" },
                new Dictionary<string, object>() { ["id"] = 3, ["name"] = "D" } });
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void StageNotApplied()
        {
            var topic = TestExecuter.GetTopic();
            var builder = new ProcessBuilder()
            {
                InputProcess = TestData.Person(topic),
                Mutators = new MutatorList()
                {
                    new CustomMutator(topic, null)
                    {
                        Then = (proc, row) =>
                        {
                            row.SetStagedValue("test", "test");
                            if (row.GetAs<int>("id") < 4)
                                row.ApplyStaging();

                            return true;
                        }
                    }
                },
            };

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(4, result.MutatedRows.Count);
            Assert.That.OrderedMatch(result, new List<Dictionary<string, object>>() {
                new Dictionary<string, object>() { ["id"] = 0, ["name"] = "A", ["test"] = "test" },
                new Dictionary<string, object>() { ["id"] = 1, ["name"] = "B", ["test"] = "test" },
                new Dictionary<string, object>() { ["id"] = 2, ["name"] = "C", ["test"] = "test" },
                new Dictionary<string, object>() { ["id"] = 3, ["name"] = "D", ["test"] = "test" } });
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(1, exceptions.Count);
            Assert.IsTrue(exceptions[0] is ProcessExecutionException);
        }

        [TestMethod]
        public void IfDelegate()
        {
            var topic = TestExecuter.GetTopic();
            var builder = new ProcessBuilder()
            {
                InputProcess = TestData.Person(topic),
                Mutators = new MutatorList()
                {
                    new CustomMutator(topic, null)
                    {
                        If = row => row.GetAs<int>("id") > 2,
                        Then = (proc, row) =>
                        {
                            row.SetValue("test", "test");
                            return true;
                        }
                    }
                },
            };

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(7, result.MutatedRows.Count);
            Assert.That.OrderedMatch(result, new List<Dictionary<string, object>>() {
                new Dictionary<string, object>() { ["id"] = 0, ["name"] = "A", ["test"] = null },
                new Dictionary<string, object>() { ["id"] = 1, ["name"] = "B", ["test"] = null },
                new Dictionary<string, object>() { ["id"] = 2, ["name"] = "C", ["test"] = null },
                new Dictionary<string, object>() { ["id"] = 3, ["name"] = "D", ["test"] = "test" },
                new Dictionary<string, object>() { ["id"] = 4, ["name"] = "E", ["test"] = "test" },
                new Dictionary<string, object>() { ["id"] = 5, ["name"] = "A", ["test"] = "test" },
                new Dictionary<string, object>() { ["id"] = 6, ["name"] = "fake", ["test"] = "test" } });
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }
    }
}