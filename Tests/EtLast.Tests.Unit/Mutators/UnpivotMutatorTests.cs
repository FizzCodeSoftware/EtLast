namespace FizzCode.EtLast.Tests.Unit.Mutators
{
    using System.Collections.Generic;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class UnpivotMutatorTests
    {
        [TestMethod]
        public void ThrowsInvalidProcessParameterException()
        {
            Assert.That.ThrowsInvalidProcessParameterException<UnpivotMutator>();
        }

        [TestMethod]
        public void FixColumnsIgnoreNull()
        {
            var topic = TestExecuter.GetTopic();
            var builder = new ProcessBuilder()
            {
                InputProcess = TestData.PersonalAssetsPivot(topic),
                Mutators = new MutatorList()
                {
                    new UnpivotMutator(topic, null)
                    {
                        FixColumns = new List<ColumnCopyConfiguration>()
                        {
                            new ColumnCopyConfiguration("id", "assetId"),
                            new ColumnCopyConfiguration("personName"),
                        },
                        NewColumnForDimension = "asset-kind",
                        NewColumnForValue = "amount",
                    },
                },
            };

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(11, result.MutatedRows.Count);
            Assert.That.ExactMatch(result.MutatedRows, new List<Dictionary<string, object>>() {
                new Dictionary<string, object>() { ["assetId"] = 1, ["personName"] = "A", ["asset-kind"] = "cars", ["amount"] = 1 },
                new Dictionary<string, object>() { ["assetId"] = 1, ["personName"] = "A", ["asset-kind"] = "houses", ["amount"] = 1 },
                new Dictionary<string, object>() { ["assetId"] = 1, ["personName"] = "A", ["asset-kind"] = "kids", ["amount"] = 2 },
                new Dictionary<string, object>() { ["personName"] = "C", ["asset-kind"] = "cars", ["amount"] = 2 },
                new Dictionary<string, object>() { ["personName"] = "C", ["asset-kind"] = "houses", ["amount"] = 1 },
                new Dictionary<string, object>() { ["personName"] = "C", ["asset-kind"] = "kids", ["amount"] = 3 },
                new Dictionary<string, object>() { ["assetId"] = 3, ["personName"] = "D", ["asset-kind"] = "houses", ["amount"] = 1 },
                new Dictionary<string, object>() { ["assetId"] = 3, ["personName"] = "D", ["asset-kind"] = "kids", ["amount"] = 3 },
                new Dictionary<string, object>() { ["assetId"] = 4, ["personName"] = "E", ["asset-kind"] = "cars", ["amount"] = "6" },
                new Dictionary<string, object>() { ["assetId"] = 4, ["personName"] = "E", ["asset-kind"] = "houses", ["amount"] = 1 },
                new Dictionary<string, object>() { ["assetId"] = 4, ["personName"] = "E", ["asset-kind"] = "kids", ["amount"] = 3 } });
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void BothColumnsIgnoreNull()
        {
            var topic = TestExecuter.GetTopic();
            var builder = new ProcessBuilder()
            {
                InputProcess = TestData.PersonalAssetsPivot(topic),
                Mutators = new MutatorList()
                {
                    new UnpivotMutator(topic, null)
                    {
                        FixColumns = new List<ColumnCopyConfiguration>()
                        {
                            new ColumnCopyConfiguration("id", "assetId"),
                            new ColumnCopyConfiguration("personName"),
                        },
                        NewColumnForDimension = "asset-kind",
                        NewColumnForValue = "amount",
                        ValueColumns = new[] { "cars", "houses", "kids" },
                    },
                },
            };

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(11, result.MutatedRows.Count);
            Assert.That.ExactMatch(result.MutatedRows, new List<Dictionary<string, object>>() {
                new Dictionary<string, object>() { ["assetId"] = 1, ["personName"] = "A", ["asset-kind"] = "cars", ["amount"] = 1 },
                new Dictionary<string, object>() { ["assetId"] = 1, ["personName"] = "A", ["asset-kind"] = "houses", ["amount"] = 1 },
                new Dictionary<string, object>() { ["assetId"] = 1, ["personName"] = "A", ["asset-kind"] = "kids", ["amount"] = 2 },
                new Dictionary<string, object>() { ["personName"] = "C", ["asset-kind"] = "cars", ["amount"] = 2 },
                new Dictionary<string, object>() { ["personName"] = "C", ["asset-kind"] = "houses", ["amount"] = 1 },
                new Dictionary<string, object>() { ["personName"] = "C", ["asset-kind"] = "kids", ["amount"] = 3 },
                new Dictionary<string, object>() { ["assetId"] = 3, ["personName"] = "D", ["asset-kind"] = "houses", ["amount"] = 1 },
                new Dictionary<string, object>() { ["assetId"] = 3, ["personName"] = "D", ["asset-kind"] = "kids", ["amount"] = 3 },
                new Dictionary<string, object>() { ["assetId"] = 4, ["personName"] = "E", ["asset-kind"] = "cars", ["amount"] = "6" },
                new Dictionary<string, object>() { ["assetId"] = 4, ["personName"] = "E", ["asset-kind"] = "houses", ["amount"] = 1 },
                new Dictionary<string, object>() { ["assetId"] = 4, ["personName"] = "E", ["asset-kind"] = "kids", ["amount"] = 3 } });
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void BothColumnsKeepNull()
        {
            var topic = TestExecuter.GetTopic();
            var builder = new ProcessBuilder()
            {
                InputProcess = TestData.PersonalAssetsPivot(topic),
                Mutators = new MutatorList()
                {
                    new UnpivotMutator(topic, null)
                    {
                        FixColumns = new List<ColumnCopyConfiguration>()
                        {
                            new ColumnCopyConfiguration("id", "assetId"),
                            new ColumnCopyConfiguration("personName"),
                        },
                        NewColumnForDimension = "asset-kind",
                        NewColumnForValue = "amount",
                        IgnoreIfValueIsNull = false,
                        ValueColumns = new[] { "cars", "houses", "kids" },
                    },
                },
            };

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(12, result.MutatedRows.Count);
            Assert.That.ExactMatch(result.MutatedRows, new List<Dictionary<string, object>>() {
                new Dictionary<string, object>() { ["assetId"] = 1, ["personName"] = "A", ["asset-kind"] = "cars", ["amount"] = 1 },
                new Dictionary<string, object>() { ["assetId"] = 1, ["personName"] = "A", ["asset-kind"] = "houses", ["amount"] = 1 },
                new Dictionary<string, object>() { ["assetId"] = 1, ["personName"] = "A", ["asset-kind"] = "kids", ["amount"] = 2 },
                new Dictionary<string, object>() { ["personName"] = "C", ["asset-kind"] = "cars", ["amount"] = 2 },
                new Dictionary<string, object>() { ["personName"] = "C", ["asset-kind"] = "houses", ["amount"] = 1 },
                new Dictionary<string, object>() { ["personName"] = "C", ["asset-kind"] = "kids", ["amount"] = 3 },
                new Dictionary<string, object>() { ["assetId"] = 3, ["personName"] = "D", ["asset-kind"] = "cars" },
                new Dictionary<string, object>() { ["assetId"] = 3, ["personName"] = "D", ["asset-kind"] = "houses", ["amount"] = 1 },
                new Dictionary<string, object>() { ["assetId"] = 3, ["personName"] = "D", ["asset-kind"] = "kids", ["amount"] = 3 },
                new Dictionary<string, object>() { ["assetId"] = 4, ["personName"] = "E", ["asset-kind"] = "cars", ["amount"] = "6" },
                new Dictionary<string, object>() { ["assetId"] = 4, ["personName"] = "E", ["asset-kind"] = "houses", ["amount"] = 1 },
                new Dictionary<string, object>() { ["assetId"] = 4, ["personName"] = "E", ["asset-kind"] = "kids", ["amount"] = 3 } });
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void ValueColumnsIgnoreNull()
        {
            var topic = TestExecuter.GetTopic();
            var builder = new ProcessBuilder()
            {
                InputProcess = TestData.PersonalAssetsPivot(topic),
                Mutators = new MutatorList()
                {
                    new UnpivotMutator(topic, null)
                    {
                        ValueColumns = new[] { "cars", "houses", "kids" },
                        NewColumnForDimension = "asset-kind",
                        NewColumnForValue = "amount",
                    },
                },
            };

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(11, result.MutatedRows.Count);
            Assert.That.ExactMatch(result.MutatedRows, new List<Dictionary<string, object>>() {
                new Dictionary<string, object>() { ["id"] = 1, ["personName"] = "A", ["asset-kind"] = "cars", ["amount"] = 1 },
                new Dictionary<string, object>() { ["id"] = 1, ["personName"] = "A", ["asset-kind"] = "houses", ["amount"] = 1 },
                new Dictionary<string, object>() { ["id"] = 1, ["personName"] = "A", ["asset-kind"] = "kids", ["amount"] = 2 },
                new Dictionary<string, object>() { ["personName"] = "C", ["asset-kind"] = "cars", ["amount"] = 2 },
                new Dictionary<string, object>() { ["personName"] = "C", ["asset-kind"] = "houses", ["amount"] = 1 },
                new Dictionary<string, object>() { ["personName"] = "C", ["asset-kind"] = "kids", ["amount"] = 3 },
                new Dictionary<string, object>() { ["id"] = 3, ["personName"] = "D", ["asset-kind"] = "houses", ["amount"] = 1 },
                new Dictionary<string, object>() { ["id"] = 3, ["personName"] = "D", ["asset-kind"] = "kids", ["amount"] = 3 },
                new Dictionary<string, object>() { ["id"] = 4, ["personName"] = "E", ["asset-kind"] = "cars", ["amount"] = "6" },
                new Dictionary<string, object>() { ["id"] = 4, ["personName"] = "E", ["asset-kind"] = "houses", ["amount"] = 1 },
                new Dictionary<string, object>() { ["id"] = 4, ["personName"] = "E", ["asset-kind"] = "kids", ["amount"] = 3 } });
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void ValueColumnsKeepNull()
        {
            var topic = TestExecuter.GetTopic();
            var builder = new ProcessBuilder()
            {
                InputProcess = TestData.PersonalAssetsPivot(topic),
                Mutators = new MutatorList()
                {
                    new UnpivotMutator(topic, null)
                    {
                        NewColumnForDimension = "asset-kind",
                        NewColumnForValue = "amount",
                        IgnoreIfValueIsNull = false,
                        ValueColumns = new[] { "cars", "houses", "kids" },
                    },
                },
            };

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(12, result.MutatedRows.Count);
            Assert.That.ExactMatch(result.MutatedRows, new List<Dictionary<string, object>>() {
                new Dictionary<string, object>() { ["id"] = 1, ["personName"] = "A", ["asset-kind"] = "cars", ["amount"] = 1 },
                new Dictionary<string, object>() { ["id"] = 1, ["personName"] = "A", ["asset-kind"] = "houses", ["amount"] = 1 },
                new Dictionary<string, object>() { ["id"] = 1, ["personName"] = "A", ["asset-kind"] = "kids", ["amount"] = 2 },
                new Dictionary<string, object>() { ["personName"] = "C", ["asset-kind"] = "cars", ["amount"] = 2 },
                new Dictionary<string, object>() { ["personName"] = "C", ["asset-kind"] = "houses", ["amount"] = 1 },
                new Dictionary<string, object>() { ["personName"] = "C", ["asset-kind"] = "kids", ["amount"] = 3 },
                new Dictionary<string, object>() { ["id"] = 3, ["personName"] = "D", ["asset-kind"] = "cars" },
                new Dictionary<string, object>() { ["id"] = 3, ["personName"] = "D", ["asset-kind"] = "houses", ["amount"] = 1 },
                new Dictionary<string, object>() { ["id"] = 3, ["personName"] = "D", ["asset-kind"] = "kids", ["amount"] = 3 },
                new Dictionary<string, object>() { ["id"] = 4, ["personName"] = "E", ["asset-kind"] = "cars", ["amount"] = "6" },
                new Dictionary<string, object>() { ["id"] = 4, ["personName"] = "E", ["asset-kind"] = "houses", ["amount"] = 1 },
                new Dictionary<string, object>() { ["id"] = 4, ["personName"] = "E", ["asset-kind"] = "kids", ["amount"] = 3 } });
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }
    }
}