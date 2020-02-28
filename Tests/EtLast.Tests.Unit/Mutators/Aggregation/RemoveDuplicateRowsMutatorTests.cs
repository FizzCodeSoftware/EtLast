namespace FizzCode.EtLast.Tests.Unit
{
    using System.Collections.Generic;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class RemoveDuplicateRowsMutatorTests
    {
        [TestMethod]
        public void ThrowsInvalidProcessParameterException()
        {
            Assert.That.ThrowsInvalidProcessParameterException<RemoveDuplicateRowsMutator>();
        }

        [TestMethod]
        public void RemoveDuplicates1()
        {
            var topic = TestExecuter.GetTopic();
            var builder = new ProcessBuilder()
            {
                InputProcess = TestData.Person(topic),
                Mutators = new MutatorList()
                {
                    new RemoveDuplicateRowsMutator(topic, null)
                    {
                        GroupingColumns = ColumnCopyConfiguration.StraightCopy("name"),
                    },
                },
            };

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(6, result.MutatedRows.Count);
            Assert.That.OrderedMatch(result, new List<Dictionary<string, object>>() {
                new Dictionary<string, object>() { ["name"] = "A" },
                new Dictionary<string, object>() { ["name"] = "B" },
                new Dictionary<string, object>() { ["name"] = "C" },
                new Dictionary<string, object>() { ["name"] = "D" },
                new Dictionary<string, object>() { ["name"] = "E" },
                new Dictionary<string, object>() { ["name"] = "fake" } });
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void RemoveDuplicates2()
        {
            var topic = TestExecuter.GetTopic();
            var builder = new ProcessBuilder()
            {
                InputProcess = TestData.Person(topic),
                Mutators = new MutatorList()
                {
                    new RemoveDuplicateRowsMutator(topic, null)
                    {
                        GroupingColumns = ColumnCopyConfiguration.StraightCopy("id", "name"),
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
        public void RemoveDuplicates3()
        {
            var topic = TestExecuter.GetTopic();
            var builder = new ProcessBuilder()
            {
                InputProcess = TestData.Person(topic),
                Mutators = new MutatorList()
                {
                    new RemoveDuplicateRowsMutator(topic, null)
                    {
                        GroupingColumns = ColumnCopyConfiguration.StraightCopy("eyeColor"),
                    },
                },
            };

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(4, result.MutatedRows.Count);
            Assert.That.OrderedMatch(result, new List<Dictionary<string, object>>() {
                new Dictionary<string, object>() { ["eyeColor"] = "brown" },
                new Dictionary<string, object>() { ["eyeColor"] = null },
                new Dictionary<string, object>() { ["eyeColor"] = "green" },
                new Dictionary<string, object>() { ["eyeColor"] = "fake" } });
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }
    }
}