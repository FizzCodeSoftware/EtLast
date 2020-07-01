namespace FizzCode.EtLast.Tests.Unit.Mutators.Aggregation
{
    using System.Collections.Generic;
    using FizzCode.LightWeight.Collections;
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
        public void SingleKey()
        {
            var topic = TestExecuter.GetTopic();
            var builder = new ProcessBuilder()
            {
                InputProcess = TestData.Person(topic),
                Mutators = new MutatorList()
                {
                    new RemoveDuplicateRowsMutator(topic, null)
                    {
                        KeyColumns = new[] { "name" },
                    },
                },
            };

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(6, result.MutatedRows.Count);
            Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
                new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "A" },
                new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "B" },
                new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "C" },
                new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "D" },
                new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "E" },
                new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "fake" } });
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void CompositeKey()
        {
            var topic = TestExecuter.GetTopic();
            var builder = new ProcessBuilder()
            {
                InputProcess = TestData.Person(topic),
                Mutators = new MutatorList()
                {
                    new RemoveDuplicateRowsMutator(topic, null)
                    {
                        KeyColumns = new[] { "id", "name" },
                    },
                },
            };

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(7, result.MutatedRows.Count);
            Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 0, ["name"] = "A" },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 1, ["name"] = "B" },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 2, ["name"] = "C" },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 3, ["name"] = "D" },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 4, ["name"] = "E" },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 5, ["name"] = "A" },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 6, ["name"] = "fake" } });
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void NullKey()
        {
            var topic = TestExecuter.GetTopic();
            var builder = new ProcessBuilder()
            {
                InputProcess = TestData.Person(topic),
                Mutators = new MutatorList()
                {
                    new RemoveDuplicateRowsMutator(topic, null)
                    {
                        KeyColumns = new[] { "eyeColor" },
                    },
                },
            };

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(4, result.MutatedRows.Count);
            Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
                new CaseInsensitiveStringKeyDictionary<object>() { ["eyeColor"] = "brown" },
                new CaseInsensitiveStringKeyDictionary<object>(),
                new CaseInsensitiveStringKeyDictionary<object>() { ["eyeColor"] = "green" },
                new CaseInsensitiveStringKeyDictionary<object>() { ["eyeColor"] = "fake" } });
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }
    }
}