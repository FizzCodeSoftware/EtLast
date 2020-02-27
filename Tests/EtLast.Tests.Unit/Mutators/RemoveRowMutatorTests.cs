namespace FizzCode.EtLast.Tests.Unit
{
    using System.Collections.Generic;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class RemoveRowMutatorTests
    {
        [TestMethod]
        public void ThrowsInvalidProcessParameterException()
        {
            Assert.That.ThrowsInvalidProcessParameterException<RemoveRowMutator>();
        }

        [TestMethod]
        public void DelegateThrowsExceptionIf()
        {
            var topic = TestExecuter.GetTopic();
            var builder = new ProcessBuilder()
            {
                InputProcess = TestData.Person(topic),
                Mutators = new MutatorList()
                {
                    new RemoveRowMutator(topic, null)
                    {
                        If = row => row.GetAs<int>("id") < 4 ? false : (row.GetAs<double>("id") == 7.0d),
                    },
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
            Assert.AreEqual(1, exceptions.Count);
            Assert.IsTrue(exceptions[0] is ProcessExecutionException);
        }

        [TestMethod]
        public void RemoveAll()
        {
            var topic = TestExecuter.GetTopic();
            var builder = new ProcessBuilder()
            {
                InputProcess = TestData.Person(topic),
                Mutators = new MutatorList()
                {
                    new RemoveRowMutator(topic, null)
                    {
                        If = row => true,
                    },
                },
            };

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(0, result.MutatedRows.Count);
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void RemoveNone()
        {
            var topic = TestExecuter.GetTopic();
            var builder = new ProcessBuilder()
            {
                InputProcess = TestData.Person(topic),
                Mutators = new MutatorList()
                {
                    new RemoveRowMutator(topic, null)
                    {
                        If = row => false,
                    },
                },
            };

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(7, result.MutatedRows.Count);
            Assert.That.OrderedMatch(result, new List<Dictionary<string, object>>() {
                new Dictionary<string, object>() { ["id"] = 0, ["name"] = "A" },
                new Dictionary<string, object>() { ["id"] = 1, ["name"] = "B" },
                new Dictionary<string, object>() { ["id"] = 2, ["name"] = "C" },
                new Dictionary<string, object>() { ["id"] = 3, ["name"] = "D" },
                new Dictionary<string, object>() { ["id"] = 4, ["name"] = "E" },
                new Dictionary<string, object>() { ["id"] = 5, ["name"] = "A" },
                new Dictionary<string, object>() { ["id"] = 6, ["name"] = "fake" } });
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void RemoveSome()
        {
            var topic = TestExecuter.GetTopic();
            var builder = new ProcessBuilder()
            {
                InputProcess = TestData.Person(topic),
                Mutators = new MutatorList()
                {
                    new RemoveRowMutator(topic, null)
                    {
                        If = row => row.GetAs<string>("name") == "A",
                    },
                },
            };

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(5, result.MutatedRows.Count);
            Assert.That.OrderedMatch(result, new List<Dictionary<string, object>>() {
                new Dictionary<string, object>() { ["id"] = 1, ["name"] = "B" },
                new Dictionary<string, object>() { ["id"] = 2, ["name"] = "C" },
                new Dictionary<string, object>() { ["id"] = 3, ["name"] = "D" },
                new Dictionary<string, object>() { ["id"] = 4, ["name"] = "E" },
                new Dictionary<string, object>() { ["id"] = 6, ["name"] = "fake" } });
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }
    }
}