namespace FizzCode.EtLast.Tests.Unit.Mutators
{
    using System.Collections.Generic;
    using FizzCode.LightWeight.Collections;
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
            var context = TestExecuter.GetContext();
            var builder = new ProcessBuilder()
            {
                InputProcess = TestData.PersonalAssetsPivot(context),
                Mutators = new MutatorList()
                {
                    new UnpivotMutator(context)
                    {
                        FixColumns = new()
                        {
                            ["assetId"] = "id",
                            ["personName"] = null
                        },
                        NewColumnForDimension = "asset-kind",
                        NewColumnForValue = "amount",
                    },
                },
            };

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(11, result.MutatedRows.Count);
            Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
                new CaseInsensitiveStringKeyDictionary<object>() { ["assetId"] = 1, ["personName"] = "A", ["asset-kind"] = "cars", ["amount"] = 1 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["assetId"] = 1, ["personName"] = "A", ["asset-kind"] = "houses", ["amount"] = 1 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["assetId"] = 1, ["personName"] = "A", ["asset-kind"] = "kids", ["amount"] = 2 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["personName"] = "C", ["asset-kind"] = "cars", ["amount"] = 2 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["personName"] = "C", ["asset-kind"] = "houses", ["amount"] = 1 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["personName"] = "C", ["asset-kind"] = "kids", ["amount"] = 3 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["assetId"] = 3, ["personName"] = "D", ["asset-kind"] = "houses", ["amount"] = 1 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["assetId"] = 3, ["personName"] = "D", ["asset-kind"] = "kids", ["amount"] = 3 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["assetId"] = 4, ["personName"] = "E", ["asset-kind"] = "cars", ["amount"] = "6" },
                new CaseInsensitiveStringKeyDictionary<object>() { ["assetId"] = 4, ["personName"] = "E", ["asset-kind"] = "houses", ["amount"] = 1 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["assetId"] = 4, ["personName"] = "E", ["asset-kind"] = "kids", ["amount"] = 3 } });
            var exceptions = context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void BothColumnsIgnoreNull()
        {
            var context = TestExecuter.GetContext();
            var builder = new ProcessBuilder()
            {
                InputProcess = TestData.PersonalAssetsPivot(context),
                Mutators = new MutatorList()
                {
                    new UnpivotMutator(context)
                    {
                        FixColumns = new()
                        {
                            ["assetId"] = "id",
                            ["personName"] = null
                        },
                        NewColumnForDimension = "asset-kind",
                        NewColumnForValue = "amount",
                        ValueColumns = new[] { "cars", "houses", "kids" },
                    },
                },
            };

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(11, result.MutatedRows.Count);
            Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
                new CaseInsensitiveStringKeyDictionary<object>() { ["assetId"] = 1, ["personName"] = "A", ["asset-kind"] = "cars", ["amount"] = 1 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["assetId"] = 1, ["personName"] = "A", ["asset-kind"] = "houses", ["amount"] = 1 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["assetId"] = 1, ["personName"] = "A", ["asset-kind"] = "kids", ["amount"] = 2 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["personName"] = "C", ["asset-kind"] = "cars", ["amount"] = 2 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["personName"] = "C", ["asset-kind"] = "houses", ["amount"] = 1 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["personName"] = "C", ["asset-kind"] = "kids", ["amount"] = 3 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["assetId"] = 3, ["personName"] = "D", ["asset-kind"] = "houses", ["amount"] = 1 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["assetId"] = 3, ["personName"] = "D", ["asset-kind"] = "kids", ["amount"] = 3 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["assetId"] = 4, ["personName"] = "E", ["asset-kind"] = "cars", ["amount"] = "6" },
                new CaseInsensitiveStringKeyDictionary<object>() { ["assetId"] = 4, ["personName"] = "E", ["asset-kind"] = "houses", ["amount"] = 1 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["assetId"] = 4, ["personName"] = "E", ["asset-kind"] = "kids", ["amount"] = 3 } });
            var exceptions = context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void BothColumnsKeepNull()
        {
            var context = TestExecuter.GetContext();
            var builder = new ProcessBuilder()
            {
                InputProcess = TestData.PersonalAssetsPivot(context),
                Mutators = new MutatorList()
                {
                    new UnpivotMutator(context)
                    {
                        FixColumns = new()
                        {
                            ["assetId"] = "id",
                            ["personName"] = null,
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
            Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
                new CaseInsensitiveStringKeyDictionary<object>() { ["assetId"] = 1, ["personName"] = "A", ["asset-kind"] = "cars", ["amount"] = 1 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["assetId"] = 1, ["personName"] = "A", ["asset-kind"] = "houses", ["amount"] = 1 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["assetId"] = 1, ["personName"] = "A", ["asset-kind"] = "kids", ["amount"] = 2 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["personName"] = "C", ["asset-kind"] = "cars", ["amount"] = 2 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["personName"] = "C", ["asset-kind"] = "houses", ["amount"] = 1 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["personName"] = "C", ["asset-kind"] = "kids", ["amount"] = 3 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["assetId"] = 3, ["personName"] = "D", ["asset-kind"] = "cars" },
                new CaseInsensitiveStringKeyDictionary<object>() { ["assetId"] = 3, ["personName"] = "D", ["asset-kind"] = "houses", ["amount"] = 1 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["assetId"] = 3, ["personName"] = "D", ["asset-kind"] = "kids", ["amount"] = 3 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["assetId"] = 4, ["personName"] = "E", ["asset-kind"] = "cars", ["amount"] = "6" },
                new CaseInsensitiveStringKeyDictionary<object>() { ["assetId"] = 4, ["personName"] = "E", ["asset-kind"] = "houses", ["amount"] = 1 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["assetId"] = 4, ["personName"] = "E", ["asset-kind"] = "kids", ["amount"] = 3 } });
            var exceptions = context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void ValueColumnsIgnoreNull()
        {
            var context = TestExecuter.GetContext();
            var builder = new ProcessBuilder()
            {
                InputProcess = TestData.PersonalAssetsPivot(context),
                Mutators = new MutatorList()
                {
                    new UnpivotMutator(context)
                    {
                        ValueColumns = new[] { "cars", "houses", "kids" },
                        NewColumnForDimension = "asset-kind",
                        NewColumnForValue = "amount",
                    },
                },
            };

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(11, result.MutatedRows.Count);
            Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 1, ["personName"] = "A", ["asset-kind"] = "cars", ["amount"] = 1 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 1, ["personName"] = "A", ["asset-kind"] = "houses", ["amount"] = 1 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 1, ["personName"] = "A", ["asset-kind"] = "kids", ["amount"] = 2 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["personName"] = "C", ["asset-kind"] = "cars", ["amount"] = 2 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["personName"] = "C", ["asset-kind"] = "houses", ["amount"] = 1 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["personName"] = "C", ["asset-kind"] = "kids", ["amount"] = 3 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 3, ["personName"] = "D", ["asset-kind"] = "houses", ["amount"] = 1 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 3, ["personName"] = "D", ["asset-kind"] = "kids", ["amount"] = 3 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 4, ["personName"] = "E", ["asset-kind"] = "cars", ["amount"] = "6" },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 4, ["personName"] = "E", ["asset-kind"] = "houses", ["amount"] = 1 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 4, ["personName"] = "E", ["asset-kind"] = "kids", ["amount"] = 3 } });
            var exceptions = context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void ValueColumnsKeepNull()
        {
            var context = TestExecuter.GetContext();
            var builder = new ProcessBuilder()
            {
                InputProcess = TestData.PersonalAssetsPivot(context),
                Mutators = new MutatorList()
                {
                    new UnpivotMutator(context)
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
            Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 1, ["personName"] = "A", ["asset-kind"] = "cars", ["amount"] = 1 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 1, ["personName"] = "A", ["asset-kind"] = "houses", ["amount"] = 1 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 1, ["personName"] = "A", ["asset-kind"] = "kids", ["amount"] = 2 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["personName"] = "C", ["asset-kind"] = "cars", ["amount"] = 2 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["personName"] = "C", ["asset-kind"] = "houses", ["amount"] = 1 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["personName"] = "C", ["asset-kind"] = "kids", ["amount"] = 3 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 3, ["personName"] = "D", ["asset-kind"] = "cars" },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 3, ["personName"] = "D", ["asset-kind"] = "houses", ["amount"] = 1 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 3, ["personName"] = "D", ["asset-kind"] = "kids", ["amount"] = 3 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 4, ["personName"] = "E", ["asset-kind"] = "cars", ["amount"] = "6" },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 4, ["personName"] = "E", ["asset-kind"] = "houses", ["amount"] = 1 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 4, ["personName"] = "E", ["asset-kind"] = "kids", ["amount"] = 3 } });
            var exceptions = context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }
    }
}