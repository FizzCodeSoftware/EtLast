namespace FizzCode.EtLast.Tests.Unit
{
    using System.Collections.Generic;
    using System.Linq;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class RemoveColumnMutatorTests
    {
        [TestMethod]
        public void ThrowsInvalidProcessParameterException()
        {
            Assert.That.ThrowsInvalidProcessParameterException<RemoveColumnMutator>();
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
                    new RemoveColumnMutator(topic, null)
                    {
                        Columns = TestData.PersonColumns,
                    },
                },
            };

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(0, result.MutatedRows.Sum(x => x.ColumnCount));
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
                    new RemoveColumnMutator(topic, null)
                    {
                        Columns = new[] { "name", "eyeColor" },
                    },
                },
            };

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(7, result.MutatedRows.Count);
            Assert.That.OrderedMatch(result, new List<Dictionary<string, object>>() {
                new Dictionary<string, object>() { ["id"] = 0, ["name"] = null, ["eyeColor"] = null },
                new Dictionary<string, object>() { ["id"] = 1, ["name"] = null, ["eyeColor"] = null },
                new Dictionary<string, object>() { ["id"] = 2, ["name"] = null, ["eyeColor"] = null },
                new Dictionary<string, object>() { ["id"] = 3, ["name"] = null, ["eyeColor"] = null },
                new Dictionary<string, object>() { ["id"] = 4, ["name"] = null, ["eyeColor"] = null },
                new Dictionary<string, object>() { ["id"] = 5, ["name"] = null, ["eyeColor"] = null },
                new Dictionary<string, object>() { ["id"] = 6, ["name"] = null, ["eyeColor"] = null } });
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }
    }
}